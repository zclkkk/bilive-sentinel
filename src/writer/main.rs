mod batch;

use anyhow::Result;
use batch::{FlushOutcome, try_flush};
use bilive_sentinel::clickhouse::{ClickHouseWriter, DanmakuRow, GiftRow};
use bilive_sentinel::protocol::{DanmakuEvent, GiftEvent};
use bilive_sentinel::redpanda::{DANMAKU_TOPIC, GIFT_TOPIC, LiveMessage, RedpandaConsumer};
use bilive_sentinel::{Config, init_tracing, new_service_registry, start_metrics_server};
use clap::Parser;
use rdkafka::message::{Message, OwnedMessage};
use std::time::Duration;

const BATCH_SIZE: usize = 100;
const BATCH_TIMEOUT: Duration = Duration::from_secs(5);

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

    let mut danmaku_buf: Vec<DanmakuRow> = Vec::new();
    let mut gift_buf: Vec<GiftRow> = Vec::new();
    let mut last_danmaku_msg: Option<OwnedMessage> = None;
    let mut last_gift_msg: Option<OwnedMessage> = None;
    let mut flush_interval = tokio::time::interval(BATCH_TIMEOUT);

    loop {
        tokio::select! {
            msg = consumer.recv() => {
                let msg = msg.map_err(|e| anyhow::anyhow!(e))?;
                let topic = msg.topic().to_string();
                let payload = match msg.payload() {
                    Some(p) => p,
                    None => {
                        tracing::warn!("received message with no payload, skipping");
                        continue;
                    }
                };

                match topic.as_str() {
                    DANMAKU_TOPIC => {
                        match serde_json::from_slice::<LiveMessage<DanmakuEvent>>(payload) {
                            Ok(wrapper) => {
                                danmaku_buf.push(danmaku_to_row(&wrapper));
                                last_danmaku_msg = Some(msg);
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "failed to deserialize danmaku payload");
                            }
                        }
                    }
                    GIFT_TOPIC => {
                        match serde_json::from_slice::<LiveMessage<GiftEvent>>(payload) {
                            Ok(wrapper) => {
                                gift_buf.push(gift_to_row(&wrapper));
                                last_gift_msg = Some(msg);
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "failed to deserialize gift payload");
                            }
                        }
                    }
                    _ => {
                        tracing::warn!(topic, "message on unknown topic");
                    }
                }

                if danmaku_buf.len() >= BATCH_SIZE {
                    flush_danmaku(&ch, &consumer, &mut danmaku_buf, &mut last_danmaku_msg).await;
                }
                if gift_buf.len() >= BATCH_SIZE {
                    flush_gifts(&ch, &consumer, &mut gift_buf, &mut last_gift_msg).await;
                }
            }
            _ = flush_interval.tick() => {
                if !danmaku_buf.is_empty() {
                    flush_danmaku(&ch, &consumer, &mut danmaku_buf, &mut last_danmaku_msg).await;
                }
                if !gift_buf.is_empty() {
                    flush_gifts(&ch, &consumer, &mut gift_buf, &mut last_gift_msg).await;
                }
            }
        }
    }
}

async fn flush_danmaku(
    ch: &ClickHouseWriter,
    consumer: &RedpandaConsumer,
    buf: &mut Vec<DanmakuRow>,
    last_msg: &mut Option<OwnedMessage>,
) {
    let insert_result = ch.insert_danmaku(buf).await.map_err(|e| e.to_string());
    let msg_ref = last_msg.as_ref();
    let outcome = try_flush(buf, insert_result, || {
        if let Some(msg) = msg_ref {
            consumer.commit(msg)
        } else {
            Ok(())
        }
    });
    if matches!(outcome, FlushOutcome::Committed) {
        last_msg.take();
    }
}

async fn flush_gifts(
    ch: &ClickHouseWriter,
    consumer: &RedpandaConsumer,
    buf: &mut Vec<GiftRow>,
    last_msg: &mut Option<OwnedMessage>,
) {
    let insert_result = ch.insert_gifts(buf).await.map_err(|e| e.to_string());
    let msg_ref = last_msg.as_ref();
    let outcome = try_flush(buf, insert_result, || {
        if let Some(msg) = msg_ref {
            consumer.commit(msg)
        } else {
            Ok(())
        }
    });
    if matches!(outcome, FlushOutcome::Committed) {
        last_msg.take();
    }
}

fn danmaku_to_row(wrapper: &LiveMessage<DanmakuEvent>) -> DanmakuRow {
    DanmakuRow {
        room_id: wrapper.room_id,
        uid: wrapper.event.uid,
        uname: wrapper.event.uname.clone(),
        message: wrapper.event.message.clone(),
        timestamp: wrapper.event.timestamp,
        command_type: wrapper.event.command_type.clone(),
        parser_version: wrapper.event.parser_version,
        received_at: wrapper.received_at,
    }
}

fn gift_to_row(wrapper: &LiveMessage<GiftEvent>) -> GiftRow {
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
    }
}
