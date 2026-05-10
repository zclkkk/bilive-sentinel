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

fn unique_base_room_id() -> u64 {
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // Use lower digits of nanosecond timestamp to avoid collision across runs
    (ns % 1_000_000_000) as u64 * 100
}

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
    let base_room_id = unique_base_room_id();
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let consumer_group = format!("test-stability-{ns}");

    ensure_topics(BOOTSTRAP_SERVERS)
        .await
        .expect("ensure_topics");
    let producer = RedpandaProducer::new(BOOTSTRAP_SERVERS);
    let ch = ClickHouseWriter::new(CLICKHOUSE_URL);
    ch.create_tables().await.expect("create_tables");

    // Clean previous test data for this room range
    let client = reqwest::Client::new();
    let min_room = base_room_id + 1;
    let max_room = base_room_id + NUM_ROOMS as u64;
    client
        .post(format!(
            "{CLICKHOUSE_URL}/?query=DELETE+FROM+bilibili_live_danmaku+WHERE+room_id+>={min_room}+AND+room_id+<={max_room}"
        ))
        .send()
        .await
        .expect("delete danmaku");
    client
        .post(format!(
            "{CLICKHOUSE_URL}/?query=DELETE+FROM+bilibili_live_gifts+WHERE+room_id+>={min_room}+AND+room_id+<={max_room}"
        ))
        .send()
        .await
        .expect("delete gifts");

    // Spawn synthetic room tasks
    let mut handles = Vec::new();
    for room_id in (base_room_id + 1)..=(base_room_id + NUM_ROOMS as u64) {
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
    let consumer = RedpandaConsumer::new(BOOTSTRAP_SERVERS, &consumer_group);
    consumer.subscribe(&[DANMAKU_TOPIC, GIFT_TOPIC]);

    let total_expected = NUM_ROOMS * EVENTS_PER_ROOM;
    let mut danmaku_count = 0usize;
    let mut gift_count = 0usize;
    let idle = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(idle);

    let mut danmaku_buf: Vec<bilive_sentinel::clickhouse::DanmakuRow> = Vec::new();
    let mut gift_buf: Vec<bilive_sentinel::clickhouse::GiftRow> = Vec::new();

    loop {
        tokio::select! {
            _ = &mut idle => {
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
                        if let Ok(wrapper) = serde_json::from_slice::<LiveMessage<DanmakuEvent>>(payload)
                            && wrapper.room_id >= min_room && wrapper.room_id <= max_room
                        {
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
                        if let Ok(wrapper) = serde_json::from_slice::<LiveMessage<GiftEvent>>(payload)
                            && wrapper.room_id >= min_room && wrapper.room_id <= max_room
                        {
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
                                received_at: wrapper.received_at,
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

                // Reset idle timer on each message
                idle.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(2));
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
    assert_eq!(danmaku_count, total_expected, "danmaku count mismatch");
    assert_eq!(gift_count, total_expected, "gift count mismatch");

    // Verify ClickHouse has records for this room range only
    let query = format!(
        "SELECT+count()+FROM+bilibili_live_danmaku+WHERE+room_id+>={min_room}+AND+room_id+<={max_room}"
    );
    let resp = client
        .get(format!("{CLICKHOUSE_URL}/?query={query}"))
        .send()
        .await
        .expect("query danmaku count");
    let body = resp.text().await.expect("body");
    let ch_danmaku: u64 = body.trim().parse().expect("parse count");
    assert_eq!(
        ch_danmaku, total_expected as u64,
        "ClickHouse danmaku count mismatch"
    );

    let query = format!(
        "SELECT+count()+FROM+bilibili_live_gifts+WHERE+room_id+>={min_room}+AND+room_id+<={max_room}"
    );
    let resp = client
        .get(format!("{CLICKHOUSE_URL}/?query={query}"))
        .send()
        .await
        .expect("query gift count");
    let body = resp.text().await.expect("body");
    let ch_gifts: u64 = body.trim().parse().expect("parse count");
    assert_eq!(
        ch_gifts, total_expected as u64,
        "ClickHouse gifts count mismatch"
    );
}
