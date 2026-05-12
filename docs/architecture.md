# Architecture

本文说明 bilive-sentinel 当前是什么、各组件负责什么、数据如何流动，以及关键正确性语义。运行和验收步骤见 [operations.md](operations.md)。

## 项目边界

bilive-sentinel 是录制管线：

- 从 Bilibili 直播间采集弹幕和礼物
- 把采集端和写入端用 Redpanda 解耦
- 把规范化事件批量写入 ClickHouse
- 用 PostgreSQL 管理房间和 collector lease
- 暴露最小 HTTP API 和 Prometheus 指标

它当前不做：

- 不提供查询 UI 或分析 API
- 不解析所有 Bilibili 直播事件
- 不保证 exactly-once
- 不在管理 API 上做认证
- 不把 `room_status` topic 投入生产路径

## 总体数据流

```text
Bilibili control-plane API
        |
        v
 LiveAuth + endpoints
        |
        v
Bilibili WebSocket
        |
        v
collector
  packet decode
  JSON extraction
  event parsing
        |
        v
Redpanda topics
        |
        v
writer
  deserialize
  batch
  insert
  commit offset
        |
        v
ClickHouse tables
```

PostgreSQL 不在事件数据面上。它只管理 rooms 和 worker leases：

```text
api  -> PostgreSQL <- collector
          rooms
          worker_leases
```

## 二进制职责

### collector

collector 拥有房间生命周期：

- 在 registry 模式下从 PostgreSQL 认领 enabled rooms
- 续租自己持有的 rooms
- 为每个 room task 获取 Bilibili live auth
- 连接 WebSocket endpoint
- 发送 auth packet 和 heartbeat packet
- 解析收到的 packet
- 发布 `DanmakuEvent` 和 `GiftEvent` 到 Redpanda
- 根据错误类型决定是否复用 auth
- 上报 room 状态和 collector metrics

collector 有三种模式：

| 模式 | 命令 | 用途 |
|------|------|------|
| registry | `cargo run --bin collector` | 正常多房间采集 |
| single room | `cargo run --bin collector -- --room-id <id>` | 直接连接一个房间，不使用 PostgreSQL registry |
| lease only | `cargo run --bin collector -- --lease-only` | 只测试 registry claim 和 renew |

### writer

writer 拥有 Redpanda 到 ClickHouse 的持久化：

- 确保 ClickHouse 表存在
- 确保 Redpanda topics 存在
- 分别运行 danmaku 和 gift 两条 topic loop
- 每条 loop 只订阅一个 topic
- 每条 loop 持有自己的 `PendingBatch`
- 批量写入 ClickHouse
- 写入成功后提交 Redpanda offset
- 对 bad 或 empty payload 只推进 offset，不写 ClickHouse row

danmaku 和 gift 在同一个 consumer group 下运行，但 commit retry 路径按 topic 隔离。某个 topic 的 batch 已写入 ClickHouse 但 offset commit 失败时，只暂停该 topic 的继续消费，另一个 topic 继续运行。

### api

api 是最小管理面：

| 方法 | 路径 | 作用 |
|------|------|------|
| GET | `/health` | 健康检查 |
| POST | `/rooms` | 添加房间 |
| GET | `/rooms` | 列出房间 |
| PUT | `/rooms/{room_id}/enable` | 启用房间 |
| PUT | `/rooms/{room_id}/disable` | 禁用房间并删除该房间 lease |
| GET | `/leases` | 列出当前 leases |

API 当前没有认证，适合本地或受保护网络。

## 协议和事件

协议层负责：

- 构造 auth packet
- 构造 heartbeat packet
- 解析 Bilibili packet header 和 body
- 处理 plain、deflate、brotli 消息体
- 从消息体中抽取 JSON
- 把支持的命令解析成类型化事件

当前支持：

| Bilibili command | 内部事件 | Redpanda topic | ClickHouse table |
|------------------|----------|----------------|------------------|
| `DANMU_MSG` | `DanmakuEvent` | `bilibili.live.danmaku.v1` | `bilibili_live_danmaku` |
| `SEND_GIFT` | `GiftEvent` | `bilibili.live.gift.v1` | `bilibili_live_gifts` |

Unsupported command 会被分类后丢弃。Malformed event 会计入 parser error，不会让 collector panic。

## Auth 复用语义

collector 不使用通用 TTL cache。每个 room task 持有一个局部 `Option<LiveAuth>`：

- 首次连接时 fetch auth
- 普通网络、协议、发布失败后复用 auth
- endpoint、auth、无 endpoint 错误后刷新 auth

这个判断来自 `RoomError`，比固定 TTL 更贴近当前领域信号。

## Registry 和 lease

PostgreSQL 表：

| 表 | 作用 |
|----|------|
| `rooms` | 房间注册、enabled 状态、last_connected_at、last_error |
| `worker_leases` | 每个房间当前由哪个 collector worker 持有 |

核心语义：

- `claim_available_rooms` 只选择 enabled rooms
- 有未过期 lease 的 room 不会被其他 worker 接管
- lease 过期后可以被其他 worker takeover
- collector 每 30 秒续租
- lease TTL 是 60 秒
- disable room 会删除对应 lease
- room task 每 5 秒检查 registry state，发现 disabled 或 lease lost 后退出

collector 进程正常 Ctrl+C 时会释放自己持有的 leases。

## Redpanda topic

| Topic | Key | Payload |
|-------|-----|---------|
| `bilibili.live.danmaku.v1` | `room_id` | JSON `LiveMessage<DanmakuEvent>` |
| `bilibili.live.gift.v1` | `room_id` | JSON `LiveMessage<GiftEvent>` |
| `bilibili.live.room_status.v1` | `room_id` | 预留，当前未生产和消费 |

topic 默认 12 partitions，replication factor 为 1，适合本地和当前 MVP 验收。

## Writer offset 语义

writer 的关键结构是 `PendingBatch`：

- `rows`: 等待写入 ClickHouse 的行
- `offsets`: 每个 topic partition 待提交的最高 next offset
- `inserted`: 当前 batch 是否已经成功写入 ClickHouse

flush 语义：

1. 没有 pending offsets：什么都不做。
2. 有 rows 且还没 inserted：先写 ClickHouse。
3. ClickHouse 写入失败：保留 batch，不提交 offset。
4. ClickHouse 写入成功：标记 inserted，然后提交 offsets。
5. offset commit 成功：清空 batch。
6. offset commit 失败：保留 batch，之后优先重试 commit。

bad 或 empty payload：

- 不添加 ClickHouse row
- 递增 `bilive_bad_messages_total`
- 通过 `advance_offset` 纳入 pending offsets
- flush 时可以走 offset-only commit

这保证 bad message 不会跳过同 partition 中还没 flush 的正常 row。

## 交付语义

writer 是 at-least-once：

- ClickHouse insert 成功后才提交 Redpanda offset
- 如果 insert 成功但 offset commit 失败，writer 会重试 commit
- 如果进程在 insert 成功、commit 成功前崩溃，重启后 Redpanda 可能重放，ClickHouse 可能出现重复记录

ClickHouse rows 带有 `source_topic`、`source_partition`、`source_offset`，可以用于诊断重复来源。

## ClickHouse 数据模型

### `bilibili_live_danmaku`

| 列 | 类型 | 说明 |
|----|------|------|
| `room_id` | UInt64 | 直播间 ID |
| `uid` | UInt64 | 用户 ID |
| `uname` | String | 用户名 |
| `message` | String | 弹幕内容 |
| `timestamp` | UInt64 | 事件时间戳 |
| `command_type` | String | 原始命令 |
| `parser_version` | UInt32 | 解析器版本 |
| `received_at` | UInt64 | collector 接收时间 |
| `source_topic` | String | Redpanda topic |
| `source_partition` | Int32 | Redpanda partition |
| `source_offset` | Int64 | Redpanda offset |

### `bilibili_live_gifts`

| 列 | 类型 | 说明 |
|----|------|------|
| `room_id` | UInt64 | 直播间 ID |
| `uid` | UInt64 | 用户 ID |
| `uname` | String | 用户名 |
| `gift_id` | UInt64 | 礼物 ID |
| `gift_name` | String | 礼物名称 |
| `coin_type` | String | `gold` 或 `silver` |
| `total_coin` | UInt64 | 总价值 |
| `num` | UInt32 | 数量 |
| `timestamp` | UInt64 | 事件时间戳 |
| `command_type` | String | 原始命令 |
| `parser_version` | UInt32 | 解析器版本 |
| `received_at` | UInt64 | collector 接收时间 |
| `source_topic` | String | Redpanda topic |
| `source_partition` | Int32 | Redpanda partition |
| `source_offset` | Int64 | Redpanda offset |

## Metrics

所有进程都会暴露 `bilive_sentinel_up`。

collector metrics：

| 指标 | 标签 | 说明 |
|------|------|------|
| `bilive_active_rooms` | 无 | 当前 active room task 数 |
| `bilive_events_total` | `type` | 已发布事件数 |
| `bilive_publish_errors_total` | `type` | 发布 Redpanda 失败次数 |
| `bilive_parser_errors_total` | 无 | 解析失败次数 |
| `bilive_reconnects_total` | 无 | 重连尝试次数 |

writer metrics：

| 指标 | 标签 | 说明 |
|------|------|------|
| `bilive_inserts_total` | `table` | ClickHouse insert 成功次数 |
| `bilive_commit_errors_total` | `table` | Redpanda offset commit 失败次数 |
| `bilive_batch_size` | 无 | insert batch 行数 |
| `bilive_insert_latency_seconds` | 无 | ClickHouse insert 耗时 |
| `bilive_consumer_lag` | `topic` | broker high watermark 与 committed offset 的差值 |
| `bilive_bad_messages_total` | `topic` | writer 反序列化失败或 empty payload 数 |

## 当前接受的限制

| 限制 | 理由 |
|------|------|
| registry 测试依赖本地 PostgreSQL | 当前测试直接验证真实 SQL 行为，比 mock 更有价值。 |
| API 没有认证 | 当前运行边界是本地或受保护网络。 |
