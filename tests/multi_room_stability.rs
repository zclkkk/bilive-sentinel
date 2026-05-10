use bilive_sentinel::clickhouse::ClickHouseWriter;
use bilive_sentinel::protocol::{DanmakuEvent, GiftEvent, PARSER_VERSION};
use bilive_sentinel::redpanda::{
    DANMAKU_TOPIC, GIFT_TOPIC, LiveMessage, RedpandaConsumer, RedpandaProducer, ensure_topics,
};
use rdkafka::message::Message;
use std::time::Duration;

const BOOTSTRAP_SERVERS: &str = "localhost:9092";
const CLICKHOUSE_URL: &str = "http://localhost:8123";
const NUM_ROOMS: usize = 5;
const EVENTS_PER_ROOM: usize = 20;

fn make_danmaku(_room_id: u64, seq: u64) -> DanmakuEvent {
    DanmakuEvent {
        uid: 1000 + seq,
        uname: format!("user_{seq}"),
        message: format!("hello {seq}"),
        timestamp: 1700000000 + seq,
        command_type: "DANMU_MSG".into(),
        parser_version: PARSER_VERSION,
    }
}

fn make_gift(_room_id: u64, seq: u64) -> GiftEvent {
    GiftEvent {
        uid: 2000 + seq,
        uname: format!("gift_user_{seq}"),
        gift_id: 100 + seq,
        gift_name: format!("gift_{seq}"),
        coin_type: "gold".into(),
        total_coin: 100,
        num: 1,
        timestamp: 1700000000 + seq,
        command_type: "SEND_GIFT".into(),
        parser_version: PARSER_VERSION,
    }
}

#[tokio::test]
async fn multi_room_publish_and_consume() {
    // Setup
    ensure_topics(BOOTSTRAP_SERVERS)
        .await
        .expect("ensure_topics");
    let producer = RedpandaProducer::new(BOOTSTRAP_SERVERS);
    let ch = ClickHouseWriter::new(CLICKHOUSE_URL);
    ch.create_tables().await.expect("create_tables");

    // Clean previous test data
    let client = reqwest::Client::new();
    client
        .post(format!(
            "{CLICKHOUSE_URL}/?query=TRUNCATE+TABLE+bilibili_live_danmaku"
        ))
        .send()
        .await
        .expect("truncate danmaku");
    client
        .post(format!(
            "{CLICKHOUSE_URL}/?query=TRUNCATE+TABLE+bilibili_live_gifts"
        ))
        .send()
        .await
        .expect("truncate gifts");

    // Spawn synthetic room tasks
    let mut handles = Vec::new();
    for room_id in 1001..=(1000 + NUM_ROOMS as u64) {
        let producer = producer.clone();
        handles.push(tokio::spawn(async move {
            for seq in 0..EVENTS_PER_ROOM as u64 {
                let danmaku = make_danmaku(room_id, seq);
                producer
                    .publish_danmaku(room_id, &danmaku)
                    .await
                    .expect("publish danmaku");

                let gift = make_gift(room_id, seq);
                producer
                    .publish_gift(room_id, &gift)
                    .await
                    .expect("publish gift");
            }
        }));
    }

    // Wait for all publishes
    for h in handles {
        h.await.expect("task join");
    }

    // Consume and insert
    let consumer = RedpandaConsumer::new(BOOTSTRAP_SERVERS, "test-stability-consumer");
    consumer.subscribe(&[DANMAKU_TOPIC, GIFT_TOPIC]);

    let total_expected = NUM_ROOMS * EVENTS_PER_ROOM;
    let mut danmaku_count = 0usize;
    let mut gift_count = 0usize;
    let timeout = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(timeout);

    let mut danmaku_buf: Vec<bilive_sentinel::clickhouse::DanmakuRow> = Vec::new();
    let mut gift_buf: Vec<bilive_sentinel::clickhouse::GiftRow> = Vec::new();

    loop {
        tokio::select! {
            _ = &mut timeout => {
                break;
            }
            msg = consumer.recv() => {
                let msg = msg.expect("recv");
                let payload = match msg.payload() {
                    Some(p) => p,
                    None => continue,
                };
                let topic = msg.topic();

                match topic {
                    DANMAKU_TOPIC => {
                        if let Ok(wrapper) = serde_json::from_slice::<LiveMessage<DanmakuEvent>>(payload) {
                            danmaku_count += 1;
                            danmaku_buf.push(bilive_sentinel::clickhouse::DanmakuRow {
                                room_id: wrapper.room_id,
                                uid: wrapper.event.uid,
                                uname: wrapper.event.uname,
                                message: wrapper.event.message,
                                timestamp: wrapper.event.timestamp,
                                command_type: wrapper.event.command_type,
                                parser_version: wrapper.event.parser_version,
                                received_at: wrapper.received_at,
                            });
                        }
                    }
                    GIFT_TOPIC => {
                        if let Ok(wrapper) = serde_json::from_slice::<LiveMessage<GiftEvent>>(payload) {
                            gift_count += 1;
                            gift_buf.push(bilive_sentinel::clickhouse::GiftRow {
                                room_id: wrapper.room_id,
                                uid: wrapper.event.uid,
                                uname: wrapper.event.uname,
                                gift_id: wrapper.event.gift_id,
                                gift_name: wrapper.event.gift_name,
                                coin_type: wrapper.event.coin_type,
                                total_coin: wrapper.event.total_coin,
                                num: wrapper.event.num,
                                timestamp: wrapper.event.timestamp,
                                command_type: wrapper.event.command_type,
                                parser_version: wrapper.event.parser_version,
                                received_at: wrapper.event.timestamp,
                            });
                        }
                    }
                    _ => {}
                }

                // Flush when buffer is full
                if danmaku_buf.len() >= 100 {
                    ch.insert_danmaku(&danmaku_buf).await.expect("insert danmaku");
                    danmaku_buf.clear();
                }
                if gift_buf.len() >= 100 {
                    ch.insert_gifts(&gift_buf).await.expect("insert gifts");
                    gift_buf.clear();
                }

                if danmaku_count >= total_expected && gift_count >= total_expected {
                    break;
                }
            }
        }
    }

    // Flush remaining
    if !danmaku_buf.is_empty() {
        ch.insert_danmaku(&danmaku_buf)
            .await
            .expect("insert danmaku final");
    }
    if !gift_buf.is_empty() {
        ch.insert_gifts(&gift_buf)
            .await
            .expect("insert gifts final");
    }

    // Verify counts
    assert!(
        danmaku_count >= total_expected,
        "expected {total_expected} danmaku, got {danmaku_count}"
    );
    assert!(
        gift_count >= total_expected,
        "expected {total_expected} gifts, got {gift_count}"
    );

    // Verify ClickHouse has records
    let resp = client
        .get(format!(
            "{CLICKHOUSE_URL}/?query=SELECT+count()+FROM+bilibili_live_danmaku"
        ))
        .send()
        .await
        .expect("query danmaku count");
    let body = resp.text().await.expect("body");
    let ch_danmaku: u64 = body.trim().parse().expect("parse count");
    assert!(
        ch_danmaku >= total_expected as u64,
        "ClickHouse danmaku: {ch_danmaku}"
    );

    let resp = client
        .get(format!(
            "{CLICKHOUSE_URL}/?query=SELECT+count()+FROM+bilibili_live_gifts"
        ))
        .send()
        .await
        .expect("query gift count");
    let body = resp.text().await.expect("body");
    let ch_gifts: u64 = body.trim().parse().expect("parse count");
    assert!(
        ch_gifts >= total_expected as u64,
        "ClickHouse gifts: {ch_gifts}"
    );
}
