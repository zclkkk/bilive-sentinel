use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

pub const DANMAKU_TOPIC: &str = "bilibili.live.danmaku.v1";
pub const GIFT_TOPIC: &str = "bilibili.live.gift.v1";
pub const ROOM_STATUS_TOPIC: &str = "bilibili.live.room_status.v1";
const TOPIC_PARTITIONS: i32 = 12;
const PRODUCER_QUEUE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveMessage<T> {
    pub room_id: u64,
    pub event: T,
    pub received_at: u64,
}

#[derive(Clone)]
pub struct RedpandaProducer {
    producer: FutureProducer,
}

impl RedpandaProducer {
    pub fn new(bootstrap_servers: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");
        Self { producer }
    }

    pub async fn publish_danmaku(
        &self,
        room_id: u64,
        event: &crate::protocol::DanmakuEvent,
    ) -> Result<(), String> {
        let msg = LiveMessage {
            room_id,
            event: event.clone(),
            received_at: now_secs(),
        };
        let payload = serde_json::to_vec(&msg).map_err(|e| e.to_string())?;
        let key = room_id.to_string();
        self.producer
            .send(
                FutureRecord::to(DANMAKU_TOPIC).key(&key).payload(&payload),
                PRODUCER_QUEUE_TIMEOUT,
            )
            .await
            .map_err(|(e, _)| e.to_string())?;
        Ok(())
    }

    pub async fn publish_gift(
        &self,
        room_id: u64,
        event: &crate::protocol::GiftEvent,
    ) -> Result<(), String> {
        let msg = LiveMessage {
            room_id,
            event: event.clone(),
            received_at: now_secs(),
        };
        let payload = serde_json::to_vec(&msg).map_err(|e| e.to_string())?;
        let key = room_id.to_string();
        self.producer
            .send(
                FutureRecord::to(GIFT_TOPIC).key(&key).payload(&payload),
                PRODUCER_QUEUE_TIMEOUT,
            )
            .await
            .map_err(|(e, _)| e.to_string())?;
        Ok(())
    }
}

pub struct RedpandaConsumer {
    consumer: StreamConsumer,
}

impl RedpandaConsumer {
    pub fn new(bootstrap_servers: &str, group_id: &str) -> Self {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Consumer creation error");
        Self { consumer }
    }

    pub fn subscribe(&self, topics: &[&str]) {
        self.consumer
            .subscribe(topics)
            .expect("Can't subscribe to topics");
    }

    pub async fn recv(&self) -> Result<OwnedMessage, String> {
        self.consumer
            .recv()
            .await
            .map_err(|e| e.to_string())
            .map(|message| message.detach())
    }

    pub fn commit(&self, message: &OwnedMessage) -> Result<(), String> {
        use rdkafka::TopicPartitionList;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(message.topic(), message.partition())
            .set_offset(rdkafka::Offset::Offset(message.offset() + 1))
            .map_err(|e| e.to_string())?;

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| e.to_string())
    }

    pub fn report_lag(&self) -> Result<HashMap<String, i64>, String> {
        let position = self.consumer.position().map_err(|e| e.to_string())?;
        let committed = self
            .consumer
            .committed(Duration::from_secs(1))
            .map_err(|e| e.to_string())?;

        let mut lag_map: HashMap<String, i64> = HashMap::new();
        for pos_elem in position.elements() {
            let pos_offset = match pos_elem.offset() {
                rdkafka::Offset::Offset(o) => o,
                _ => continue,
            };
            let commit_offset = committed
                .elements()
                .iter()
                .find(|c| c.topic() == pos_elem.topic() && c.partition() == pos_elem.partition())
                .and_then(|c| match c.offset() {
                    rdkafka::Offset::Offset(o) => Some(o),
                    _ => None,
                })
                .unwrap_or(0);
            *lag_map.entry(pos_elem.topic().to_string()).or_insert(0) += pos_offset - commit_offset;
        }
        Ok(lag_map)
    }
}

pub async fn ensure_topics(bootstrap_servers: &str) -> Result<(), String> {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .map_err(|e| e.to_string())?;

    let topics = [
        NewTopic::new(DANMAKU_TOPIC, TOPIC_PARTITIONS, TopicReplication::Fixed(1)),
        NewTopic::new(GIFT_TOPIC, TOPIC_PARTITIONS, TopicReplication::Fixed(1)),
        NewTopic::new(
            ROOM_STATUS_TOPIC,
            TOPIC_PARTITIONS,
            TopicReplication::Fixed(1),
        ),
    ];

    admin
        .create_topics(topics.iter(), &AdminOptions::new())
        .await
        .map_err(|e| e.to_string())?;

    let partitions = [
        NewPartitions::new(DANMAKU_TOPIC, TOPIC_PARTITIONS as usize),
        NewPartitions::new(GIFT_TOPIC, TOPIC_PARTITIONS as usize),
        NewPartitions::new(ROOM_STATUS_TOPIC, TOPIC_PARTITIONS as usize),
    ];
    admin
        .create_partitions(partitions.iter(), &AdminOptions::new())
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
