mod batch;

use anyhow::Result;
use batch::{FlushOutcome, PendingBatch, try_flush};
use bilive_sentinel::clickhouse::{ClickHouseWriter, DanmakuRow, GiftRow};
use bilive_sentinel::protocol::{DanmakuEvent, GiftEvent};
use bilive_sentinel::redpanda::{DANMAKU_TOPIC, GIFT_TOPIC, LiveMessage, RedpandaConsumer};
use bilive_sentinel::{Config, init_tracing, new_service_registry, start_metrics_server};
use clap::Parser;
use rdkafka::message::Message;
use std::time::Duration;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "config/default.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    init_tracing(&config.log.level);

    tracing::info!("writer starting");

    let registry = new_service_registry();
    let writer_metrics = bilive_sentinel::metrics::WriterMetrics::register(&registry);
    let metrics_addr = config.writer.metrics_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(&metrics_addr, registry).await {
            tracing::error!(error = %e, "metrics server failed");
        }
    });

    let ch = ClickHouseWriter::new(&config.clickhouse.url);
    ch.create_tables()
        .await
        .map_err(|e| anyhow::anyhow!("create_tables: {e}"))?;
    tracing::info!("clickhouse tables ready");

    bilive_sentinel::redpanda::ensure_topics(&config.redpanda.bootstrap_servers)
        .await
        .map_err(|e| anyhow::anyhow!("ensure_topics: {e}"))?;
    tracing::info!("redpanda topics ready");

    let consumer =
        RedpandaConsumer::new(&config.redpanda.bootstrap_servers, "bilive-sentinel-writer");
    consumer.subscribe(&[DANMAKU_TOPIC, GIFT_TOPIC]);
    tracing::info!("subscribed to redpanda topics");

    let batch_size = config.writer.batch_size;
    let batch_timeout = Duration::from_millis(config.writer.batch_timeout_ms);
    let commit_retry_delay = Duration::from_millis(250);

    let mut danmaku_batch: PendingBatch<DanmakuRow> = PendingBatch::new();
    let mut gift_batch: PendingBatch<GiftRow> = PendingBatch::new();
    let mut flush_interval = tokio::time::interval(batch_timeout);
    let mut lag_interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        if danmaku_batch.inserted() || gift_batch.inserted() {
            let mut commit_failed = false;
            if danmaku_batch.inserted() {
                let outcome =
                    flush_danmaku(&ch, &consumer, &mut danmaku_batch, &writer_metrics).await;
                commit_failed |= matches!(outcome, FlushOutcome::CommitFailed);
            }
            if gift_batch.inserted() {
                let outcome = flush_gifts(&ch, &consumer, &mut gift_batch, &writer_metrics).await;
                commit_failed |= matches!(outcome, FlushOutcome::CommitFailed);
            }
            if commit_failed {
                tokio::select! {
                    biased;
                    _ = lag_interval.tick() => report_lag(&consumer, &writer_metrics),
                    _ = tokio::time::sleep(commit_retry_delay) => {}
                }
            }
            continue;
        }

        tokio::select! {
            msg = consumer.recv() => {
                let msg = msg.map_err(|e| anyhow::anyhow!(e))?;
                let topic = msg.topic().to_string();
                let partition = msg.partition();
                let next_offset = msg.offset() + 1;
                let payload = match msg.payload() {
                    Some(p) => p,
                    None => {
                        tracing::warn!(topic, partition, offset = msg.offset(), "empty payload, skipping");
                        writer_metrics.bad_messages_total.with_label_values(&[&topic]).inc();
                        match topic.as_str() {
                            DANMAKU_TOPIC => danmaku_batch.advance_offset(&topic, partition, next_offset),
                            GIFT_TOPIC => gift_batch.advance_offset(&topic, partition, next_offset),
                            _ => {}
                        }
                        continue;
                    }
                };

                let consumed_offset = msg.offset();

                match topic.as_str() {
                    DANMAKU_TOPIC => {
                        match serde_json::from_slice::<LiveMessage<DanmakuEvent>>(payload) {
                            Ok(wrapper) => {
                                danmaku_batch.push(
                                    danmaku_to_row(&wrapper, &topic, partition, consumed_offset),
                                    &topic,
                                    partition,
                                    next_offset,
                                );
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, topic, "bad danmaku payload, skipping");
                                writer_metrics.bad_messages_total.with_label_values(&[&topic]).inc();
                                danmaku_batch.advance_offset(&topic, partition, next_offset);
                            }
                        }
                    }
                    GIFT_TOPIC => {
                        match serde_json::from_slice::<LiveMessage<GiftEvent>>(payload) {
                            Ok(wrapper) => {
                                gift_batch.push(
                                    gift_to_row(&wrapper, &topic, partition, consumed_offset),
                                    &topic,
                                    partition,
                                    next_offset,
                                );
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, topic, "bad gift payload, skipping");
                                writer_metrics.bad_messages_total.with_label_values(&[&topic]).inc();
                                gift_batch.advance_offset(&topic, partition, next_offset);
                            }
                        }
                    }
                    _ => {
                        tracing::warn!(topic, "message on unknown topic");
                    }
                }

                if danmaku_batch.len() >= batch_size {
                    flush_danmaku(&ch, &consumer, &mut danmaku_batch, &writer_metrics).await;
                }
                if gift_batch.len() >= batch_size {
                    flush_gifts(&ch, &consumer, &mut gift_batch, &writer_metrics).await;
                }
            }
            _ = flush_interval.tick() => {
                if danmaku_batch.has_pending_offsets() {
                    flush_danmaku(&ch, &consumer, &mut danmaku_batch, &writer_metrics).await;
                }
                if gift_batch.has_pending_offsets() {
                    flush_gifts(&ch, &consumer, &mut gift_batch, &writer_metrics).await;
                }
            }
            _ = lag_interval.tick() => {
                report_lag(&consumer, &writer_metrics);
            }
        }
    }
}

fn report_lag(consumer: &RedpandaConsumer, metrics: &bilive_sentinel::metrics::WriterMetrics) {
    match consumer.report_lag() {
        Ok(lag_map) => {
            for (topic, lag) in lag_map {
                metrics
                    .consumer_lag
                    .with_label_values(&[&topic])
                    .set(lag as f64);
            }
        }
        Err(e) => tracing::warn!(error = %e, "consumer lag report failed"),
    }
}

async fn flush_danmaku(
    ch: &ClickHouseWriter,
    consumer: &RedpandaConsumer,
    batch: &mut PendingBatch<DanmakuRow>,
    metrics: &bilive_sentinel::metrics::WriterMetrics,
) -> FlushOutcome {
    if !batch.inserted() && !batch.is_empty() {
        metrics.batch_size.observe(batch.len() as f64);
    }
    let insert_result = if batch.inserted() || batch.is_empty() {
        None
    } else {
        let start = std::time::Instant::now();
        let result = ch
            .insert_danmaku(batch.rows())
            .await
            .map_err(|e| e.to_string());
        metrics
            .insert_latency
            .observe(start.elapsed().as_secs_f64());
        Some(result)
    };
    let outcome = try_flush(batch, insert_result, |offsets| {
        consumer.commit_offsets(offsets)
    });
    if matches!(outcome, FlushOutcome::Committed) {
        metrics.inserts_total.with_label_values(&["danmaku"]).inc();
    } else if matches!(outcome, FlushOutcome::CommitFailed) {
        metrics
            .commit_errors_total
            .with_label_values(&["danmaku"])
            .inc();
    }
    outcome
}

async fn flush_gifts(
    ch: &ClickHouseWriter,
    consumer: &RedpandaConsumer,
    batch: &mut PendingBatch<GiftRow>,
    metrics: &bilive_sentinel::metrics::WriterMetrics,
) -> FlushOutcome {
    if !batch.inserted() && !batch.is_empty() {
        metrics.batch_size.observe(batch.len() as f64);
    }
    let insert_result = if batch.inserted() || batch.is_empty() {
        None
    } else {
        let start = std::time::Instant::now();
        let result = ch
            .insert_gifts(batch.rows())
            .await
            .map_err(|e| e.to_string());
        metrics
            .insert_latency
            .observe(start.elapsed().as_secs_f64());
        Some(result)
    };
    let outcome = try_flush(batch, insert_result, |offsets| {
        consumer.commit_offsets(offsets)
    });
    if matches!(outcome, FlushOutcome::Committed) {
        metrics.inserts_total.with_label_values(&["gifts"]).inc();
    } else if matches!(outcome, FlushOutcome::CommitFailed) {
        metrics
            .commit_errors_total
            .with_label_values(&["gifts"])
            .inc();
    }
    outcome
}

fn danmaku_to_row(
    wrapper: &LiveMessage<DanmakuEvent>,
    source_topic: &str,
    source_partition: i32,
    source_offset: i64,
) -> DanmakuRow {
    DanmakuRow {
        room_id: wrapper.room_id,
        uid: wrapper.event.uid,
        uname: wrapper.event.uname.clone(),
        message: wrapper.event.message.clone(),
        timestamp: wrapper.event.timestamp,
        command_type: wrapper.event.command_type.clone(),
        parser_version: wrapper.event.parser_version,
        received_at: wrapper.received_at,
        source_topic: source_topic.to_string(),
        source_partition,
        source_offset,
    }
}

fn gift_to_row(
    wrapper: &LiveMessage<GiftEvent>,
    source_topic: &str,
    source_partition: i32,
    source_offset: i64,
) -> GiftRow {
    GiftRow {
        room_id: wrapper.room_id,
        uid: wrapper.event.uid,
        uname: wrapper.event.uname.clone(),
        gift_id: wrapper.event.gift_id,
        gift_name: wrapper.event.gift_name.clone(),
        coin_type: wrapper.event.coin_type.clone(),
        total_coin: wrapper.event.total_coin,
        num: wrapper.event.num,
        timestamp: wrapper.event.timestamp,
        command_type: wrapper.event.command_type.clone(),
        parser_version: wrapper.event.parser_version,
        received_at: wrapper.received_at,
        source_topic: source_topic.to_string(),
        source_partition,
        source_offset,
    }
}
