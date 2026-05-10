use rdkafka::admin::{
    AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication, TopicResult,
};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
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
        let mut offsets = HashMap::new();
        offsets.insert(
            TopicPartition::new(message.topic(), message.partition()),
            message.offset() + 1,
        );
        self.commit_offsets(&offsets)
    }

    pub fn commit_offsets(&self, offsets: &HashMap<TopicPartition, i64>) -> Result<(), String> {
        use rdkafka::TopicPartitionList;

        if offsets.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        for (topic_partition, offset) in offsets {
            tpl.add_partition(&topic_partition.topic, topic_partition.partition)
                .set_offset(rdkafka::Offset::Offset(*offset))
                .map_err(|e| e.to_string())?;
        }

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

    let topic_results = admin
        .create_topics(topics.iter(), &AdminOptions::new())
        .await
        .map_err(|e| e.to_string())?;
    check_admin_results(
        "create topic",
        topic_results,
        &[RDKafkaErrorCode::TopicAlreadyExists],
    )?;

    ensure_partition_count(
        &admin,
        &[DANMAKU_TOPIC, GIFT_TOPIC, ROOM_STATUS_TOPIC],
        TOPIC_PARTITIONS as usize,
    )
    .await?;

    Ok(())
}

async fn ensure_partition_count<C: ClientContext>(
    admin: &AdminClient<C>,
    topic_names: &[&str],
    partition_count: usize,
) -> Result<(), String> {
    let metadata = admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(5))
        .map_err(|e| e.to_string())?;
    let partitions: Vec<_> = topic_names
        .iter()
        .filter_map(|topic| {
            let current = metadata
                .topics()
                .iter()
                .find(|metadata_topic| metadata_topic.name() == *topic)
                .map(|metadata_topic| metadata_topic.partitions().len());
            if current.is_some_and(|count| count >= partition_count) {
                None
            } else {
                Some(NewPartitions::new(topic, partition_count))
            }
        })
        .collect();

    if !partitions.is_empty() {
        let partition_results = admin
            .create_partitions(partitions.iter(), &AdminOptions::new())
            .await
            .map_err(|e| e.to_string())?;
        check_admin_results(
            "create partitions",
            partition_results,
            &[
                RDKafkaErrorCode::InvalidPartitions,
                RDKafkaErrorCode::InvalidRequest,
            ],
        )?;
    }

    let metadata = admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(5))
        .map_err(|e| e.to_string())?;
    for topic in topic_names {
        let Some(metadata_topic) = metadata
            .topics()
            .iter()
            .find(|metadata_topic| metadata_topic.name() == *topic)
        else {
            return Err(format!("topic {topic} missing after ensure"));
        };
        let current = metadata_topic.partitions().len();
        if current < partition_count {
            return Err(format!(
                "topic {topic} has {current} partitions, expected at least {partition_count}"
            ));
        }
    }

    Ok(())
}

fn check_admin_results(
    operation: &str,
    results: Vec<TopicResult>,
    allowed_errors: &[RDKafkaErrorCode],
) -> Result<(), String> {
    for result in results {
        match result {
            Ok(_) => {}
            Err((topic, code)) if allowed_errors.contains(&code) => {
                tracing::debug!(operation, topic, error = ?code, "admin result already satisfied");
            }
            Err((topic, code)) => {
                return Err(format!("{operation} failed for {topic}: {code:?}"));
            }
        }
    }
    Ok(())
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_results_allow_expected_errors() {
        let results = vec![
            Ok("topic-a".to_string()),
            Err(("topic-b".to_string(), RDKafkaErrorCode::TopicAlreadyExists)),
        ];

        assert!(
            check_admin_results(
                "create topic",
                results,
                &[RDKafkaErrorCode::TopicAlreadyExists],
            )
            .is_ok()
        );
    }

    #[test]
    fn admin_results_reject_unexpected_errors() {
        let results = vec![Err((
            "topic-a".to_string(),
            RDKafkaErrorCode::InvalidReplicationFactor,
        ))];

        let err = check_admin_results(
            "create topic",
            results,
            &[RDKafkaErrorCode::TopicAlreadyExists],
        )
        .expect_err("unexpected errors must fail");

        assert!(err.contains("InvalidReplicationFactor"));
    }
}
