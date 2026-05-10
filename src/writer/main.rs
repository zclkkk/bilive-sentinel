use anyhow::Result;
use bilive_sentinel::clickhouse::{ClickHouseWriter, DanmakuRow, GiftRow};
use bilive_sentinel::protocol::{DanmakuEvent, GiftEvent};
use bilive_sentinel::redpanda::{DANMAKU_TOPIC, GIFT_TOPIC, LiveMessage, RedpandaConsumer};
use bilive_sentinel::{Config, init_tracing, new_service_registry, start_metrics_server};
use clap::Parser;
use rdkafka::message::Message;
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
    let mut flush_interval = tokio::time::interval(BATCH_TIMEOUT);

    loop {
        tokio::select! {
            msg = consumer.recv() => {
                let msg = msg.map_err(|e| anyhow::anyhow!(e))?;
                let topic = msg.topic().to_string();
                let payload = match msg.payload() {
                    Some(p) => p,
                    None => continue,
                };

                match topic.as_str() {
                    DANMAKU_TOPIC => {
                        if let Ok(wrapper) = serde_json::from_slice::<LiveMessage<DanmakuEvent>>(payload) {
                            danmaku_buf.push(danmaku_to_row(&wrapper));
                        }
                    }
                    GIFT_TOPIC => {
                        if let Ok(wrapper) = serde_json::from_slice::<LiveMessage<GiftEvent>>(payload) {
                            gift_buf.push(gift_to_row(&wrapper));
                        }
                    }
                    _ => {}
                }

                if danmaku_buf.len() >= BATCH_SIZE {
                    if let Err(e) = ch.insert_danmaku(&danmaku_buf).await {
                        tracing::warn!(error = %e, "insert danmaku failed");
                    } else {
                        tracing::debug!(count = danmaku_buf.len(), "inserted danmaku batch");
                    }
                    danmaku_buf.clear();
                }
                if gift_buf.len() >= BATCH_SIZE {
                    if let Err(e) = ch.insert_gifts(&gift_buf).await {
                        tracing::warn!(error = %e, "insert gifts failed");
                    } else {
                        tracing::debug!(count = gift_buf.len(), "inserted gift batch");
                    }
                    gift_buf.clear();
                }
            }
            _ = flush_interval.tick() => {
                if !danmaku_buf.is_empty() {
                    if let Err(e) = ch.insert_danmaku(&danmaku_buf).await {
                        tracing::warn!(error = %e, "insert danmaku failed");
                    } else {
                        tracing::debug!(count = danmaku_buf.len(), "flushed danmaku batch");
                    }
                    danmaku_buf.clear();
                }
                if !gift_buf.is_empty() {
                    if let Err(e) = ch.insert_gifts(&gift_buf).await {
                        tracing::warn!(error = %e, "insert gifts failed");
                    } else {
                        tracing::debug!(count = gift_buf.len(), "flushed gift batch");
                    }
                    gift_buf.clear();
                }
            }
        }
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
