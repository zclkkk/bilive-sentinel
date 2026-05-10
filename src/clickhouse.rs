use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct DanmakuRow {
    pub room_id: u64,
    pub uid: u64,
    pub uname: String,
    pub message: String,
    pub timestamp: u64,
    pub command_type: String,
    pub parser_version: u32,
    pub received_at: u64,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct GiftRow {
    pub room_id: u64,
    pub uid: u64,
    pub uname: String,
    pub gift_id: u64,
    pub gift_name: String,
    pub coin_type: String,
    pub total_coin: u64,
    pub num: u32,
    pub timestamp: u64,
    pub command_type: String,
    pub parser_version: u32,
    pub received_at: u64,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
}

pub struct ClickHouseWriter {
    client: clickhouse::Client,
}

impl ClickHouseWriter {
    pub fn new(url: &str) -> Self {
        let client = clickhouse::Client::default().with_url(url);
        Self { client }
    }

    pub async fn create_tables(&self) -> Result<(), String> {
        self.client
            .query(
                "CREATE TABLE IF NOT EXISTS bilibili_live_danmaku (
                    room_id UInt64,
                    uid UInt64,
                    uname String,
                    message String,
                    timestamp UInt64,
                    command_type String,
                    parser_version UInt32,
                    received_at UInt64,
                    source_topic String,
                    source_partition Int32,
                    source_offset Int64
                ) ENGINE = MergeTree() ORDER BY (room_id, timestamp)",
            )
            .execute()
            .await
            .map_err(|e| e.to_string())?;

        self.client
            .query(
                "CREATE TABLE IF NOT EXISTS bilibili_live_gifts (
                    room_id UInt64,
                    uid UInt64,
                    uname String,
                    gift_id UInt64,
                    gift_name String,
                    coin_type String,
                    total_coin UInt64,
                    num UInt32,
                    timestamp UInt64,
                    command_type String,
                    parser_version UInt32,
                    received_at UInt64,
                    source_topic String,
                    source_partition Int32,
                    source_offset Int64
                ) ENGINE = MergeTree() ORDER BY (room_id, timestamp)",
            )
            .execute()
            .await
            .map_err(|e| e.to_string())?;

        // Migrate existing tables: add source metadata columns if missing
        for table in &["bilibili_live_danmaku", "bilibili_live_gifts"] {
            for (col, typ) in &[
                ("source_topic", "String"),
                ("source_partition", "Int32"),
                ("source_offset", "Int64"),
            ] {
                self.client
                    .query(&format!(
                        "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {typ}"
                    ))
                    .execute()
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }

        Ok(())
    }

    pub async fn insert_danmaku(&self, rows: &[DanmakuRow]) -> Result<(), String> {
        let mut insert = self
            .client
            .insert("bilibili_live_danmaku")
            .map_err(|e| e.to_string())?;
        for row in rows {
            insert.write(row).await.map_err(|e| e.to_string())?;
        }
        insert.end().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn insert_gifts(&self, rows: &[GiftRow]) -> Result<(), String> {
        let mut insert = self
            .client
            .insert("bilibili_live_gifts")
            .map_err(|e| e.to_string())?;
        for row in rows {
            insert.write(row).await.map_err(|e| e.to_string())?;
        }
        insert.end().await.map_err(|e| e.to_string())?;
        Ok(())
    }
}
