# bilive-sentinel

Bilibili 直播间弹幕和礼物录制器。通过 WebSocket 连接多个直播间采集事件，经 Redpanda 缓冲，批量写入 ClickHouse。

## 架构

```
Bilibili 直播 API  →  collector  →  Redpanda  →  writer  →  ClickHouse
     (鉴权)          (WebSocket)    (缓冲)      (批量写入)
                                          PostgreSQL
                                        (房间注册、
                                         worker 租约)
```

三个二进制：

| 二进制 | 职责 |
|--------|------|
| `collector` | 连接 Bilibili 直播间，解析事件，发布到 Redpanda |
| `writer` | 从 Redpanda 消费，批量写入 ClickHouse |
| `api` | HTTP 管理接口（房间和租约） |

## 环境要求

- Rust 2024 edition（`rustup update`）
- Podman（或兼容 compose 的 Docker）

## 快速开始

### 1. 启动基础设施

```bash
./scripts/dev-up
```

通过 Podman Compose 启动 PostgreSQL、Redpanda、ClickHouse。

### 2. 构建

```bash
cargo build
```

### 3. 运行

在三个终端分别启动：

```bash
# Writer（Redpanda → ClickHouse）
cargo run --bin writer

# Collector（连接直播间，发布到 Redpanda）
cargo run --bin collector

# API（管理接口）
cargo run --bin api
```

所有二进制默认读取 `config/default.toml`，可用 `-c` 覆盖：

```bash
cargo run --bin collector -- -c config/production.toml
```

### 4. 添加房间

```bash
curl -X POST http://localhost:8080/rooms \
  -H 'Content-Type: application/json' \
  -d '{"room_id": 21484828}'
```

### 5. 查看状态

```bash
curl http://localhost:8080/rooms
curl http://localhost:8080/leases
```

## 二进制说明

### collector

连接 Bilibili 直播间，将类型化事件发布到 Redpanda。

```
cargo run --bin collector [OPTIONS]

Options:
  -c, --config <PATH>              配置文件 [默认: config/default.toml]
      --room-id <ID>               直接连接单个房间（跳过注册表）
      --check-live-auth <ID>       打印房间鉴权信息后退出
      --capacity <N>               最大认领房间数 [默认: 100]
      --lease-only                 只认领租约不连接（测试调度器）
```

运行模式：
- **注册模式**（默认）：从 PostgreSQL 认领房间，逐个连接，断线后自动重连（指数退避）。
- **单房间模式**（`--room-id`）：直接连接一个房间，不需要 PostgreSQL。
- **仅租约模式**（`--lease-only`）：只认领和续租，不建立连接。用于测试调度逻辑。

重连行为：WebSocket 断开后，collector 复用缓存的鉴权信息重连，仅在 endpoint 失效时重新获取鉴权。退避参数可配置（`reconnect_base_ms`、`reconnect_max_ms`、`reconnect_max_retries`）。

### writer

从 Redpanda 消费事件，批量写入 ClickHouse。

```
cargo run --bin writer [OPTIONS]

Options:
  -c, --config <PATH>    配置文件 [默认: config/default.toml]
```

批量策略：缓冲满 `batch_size` 条或经过 `batch_timeout_ms` 毫秒时刷新，以先到者为准。仅在 ClickHouse 写入成功后才提交 Redpanda offset。

### api

HTTP 管理接口。

```
cargo run --bin api [OPTIONS]

Options:
  -c, --config <PATH>    配置文件 [默认: config/default.toml]
```

接口：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 返回 `ok` |
| POST | `/rooms` | 添加房间，body: `{"room_id": N}` |
| GET | `/rooms` | 列出所有房间 |
| PUT | `/rooms/{room_id}/enable` | 启用房间 |
| PUT | `/rooms/{room_id}/disable` | 禁用房间 |
| GET | `/leases` | 列出活跃 worker 租约 |

每个二进制还在各自的 `metrics_addr` 上暴露 Prometheus 指标端点。

## 配置

`config/default.toml`：

```toml
[log]
level = "info"

[collector]
metrics_addr = "0.0.0.0:9100"
startup_delay_ms = 200           # 启动时房间任务之间的延迟
api_concurrency_limit = 10       # Bilibili API 最大并发数
endpoint_rate_limit = 20         # WebSocket 最大并发建连数
reconnect_base_ms = 1000         # 退避基准延迟
reconnect_max_ms = 60000         # 退避最大延迟
reconnect_max_retries = 10       # 超过此次数后放弃重连

[writer]
metrics_addr = "0.0.0.0:9101"
batch_size = 100                 # 每批 ClickHouse 写入条数
batch_timeout_ms = 5000          # 部分批次最大等待毫秒

[api]
listen_addr = "0.0.0.0:8080"
metrics_addr = "0.0.0.0:9102"

[postgres]
url = "postgres://bilive:bilive@localhost:5432/bilive"

[clickhouse]
url = "http://localhost:8123"

[redpanda]
bootstrap_servers = "localhost:9092"
```

collector 和 writer 的所有字段都有默认值，省略不影响启动。

## 指标

每个二进制在 `http://<metrics_addr>/metrics` 暴露 Prometheus 指标。

### Collector 指标

| 指标 | 类型 | 标签 | 说明 |
|------|------|------|------|
| `bilive_active_rooms` | Gauge | — | 当前连接的房间数 |
| `bilive_events_total` | Counter | `type`（danmaku/gift） | 已处理事件总数 |
| `bilive_publish_errors_total` | Counter | `type`（danmaku/gift） | 发布到 Redpanda 失败次数 |
| `bilive_parser_errors_total` | Counter | — | 解析错误总数 |
| `bilive_reconnects_total` | Counter | — | 重连尝试总数 |

### Writer 指标

| 指标 | 类型 | 标签 | 说明 |
|------|------|------|------|
| `bilive_inserts_total` | Counter | `table`（danmaku/gifts） | ClickHouse 写入次数 |
| `bilive_commit_errors_total` | Counter | `table`（danmaku/gifts） | Redpanda offset 提交失败次数 |
| `bilive_batch_size` | Histogram | — | 写入批次大小 |
| `bilive_insert_latency_seconds` | Histogram | — | ClickHouse 写入耗时 |
| `bilive_consumer_lag` | Gauge | `topic` | broker high watermark 与已提交 offset 的差值 |
| `bilive_bad_messages_total` | Counter | `topic` | 无法反序列化的消息数（已提交 offset 跳过） |

### 全局

| 指标 | 类型 | 说明 |
|------|------|------|
| `bilive_sentinel_up` | Gauge | 运行时固定为 1 |

## 数据模型

### ClickHouse 表

**bilibili_live_danmaku**

| 列 | 类型 | 说明 |
|-----|------|------|
| room_id | UInt64 | 直播间 ID |
| uid | UInt64 | 用户 ID |
| uname | String | 用户名 |
| message | String | 弹幕内容 |
| timestamp | UInt64 | 事件时间戳（unix 秒） |
| command_type | String | 原始命令类型 |
| parser_version | UInt32 | 解析器版本 |
| received_at | UInt64 | collector 接收时间 |
| source_topic | String | Redpanda topic 名称 |
| source_partition | Int32 | Redpanda partition 编号 |
| source_offset | Int64 | Redpanda 消息 offset |

**bilibili_live_gifts**

| 列 | 类型 | 说明 |
|-----|------|------|
| room_id | UInt64 | 直播间 ID |
| uid | UInt64 | 用户 ID |
| uname | String | 用户名 |
| gift_id | UInt64 | 礼物 ID |
| gift_name | String | 礼物名称 |
| coin_type | String | 币种（gold/silver） |
| total_coin | UInt64 | 总价值 |
| num | UInt32 | 数量 |
| timestamp | UInt64 | 事件时间戳 |
| command_type | String | 原始命令类型 |
| parser_version | UInt32 | 解析器版本 |
| received_at | UInt64 | collector 接收时间 |
| source_topic | String | Redpanda topic 名称 |
| source_partition | Int32 | Redpanda partition 编号 |
| source_offset | Int64 | Redpanda 消息 offset |

### PostgreSQL 表

**rooms** — 房间注册表

| 列 | 类型 | 说明 |
|-----|------|------|
| room_id | BIGINT | 主键 |
| enabled | BOOLEAN | 是否允许 collector 连接 |
| last_connected_at | TIMESTAMP | 上次成功连接时间 |
| last_error | TEXT | 上次错误信息 |
| created_at | TIMESTAMP | 创建时间 |
| updated_at | TIMESTAMP | 最后修改时间 |

**worker_leases** — Worker 租约

| 列 | 类型 | 说明 |
|-----|------|------|
| room_id | BIGINT | 外键，关联 rooms |
| worker_id | TEXT | collector 实例 UUID |
| leased_at | TIMESTAMP | 租约开始时间 |
| expires_at | TIMESTAMP | 租约过期时间 |
| last_heartbeat | TIMESTAMP | 上次续租时间 |

### Redpanda Topic

| Topic | Key | Payload |
|-------|-----|---------|
| `bilibili.live.danmaku.v1` | room_id | `LiveMessage<DanmakuEvent>`（JSON） |
| `bilibili.live.gift.v1` | room_id | `LiveMessage<GiftEvent>`（JSON） |
| `bilibili.live.room_status.v1` | room_id | 预留房间状态事件 |

## 当前已知限制

| 限制项 | 当前是否存在 | 是否可解决 | 说明 |
|--------|--------------|------------|------|
| registry 测试依赖本地 PostgreSQL | 是 | 可以 | 目前 `registry` 单元测试直接连接 `localhost:5432`，可改为 testcontainers、临时数据库，或将依赖基础设施的测试移动到集成测试。 |
| writer commit 失败重试期间暂停消费 | 是 | 可以 | 当 batch 处于 inserted-pending-commit 状态时，writer 暂停消费新消息并重试 commit；可改为异步重试队列以减少消费暂停时间。 |
| API 没有认证 | 是 | 可以 | 本地开发可接受，但生产或公网部署前需要加鉴权、网络隔离，或放在受保护的管理网内。 |

## 测试

```bash
# 单元测试（需要 PostgreSQL，registry 测试连接本地数据库）
cargo test --lib

# 全部测试（需要 PostgreSQL、Redpanda、ClickHouse 运行）
cargo test

# 仅集成测试
cargo test --test multi_room_stability
```

## 停止

```bash
./scripts/dev-down
```
