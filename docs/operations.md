# Operations

本文只描述怎么运行、验收、监控和排障。系统设计和数据语义见 [architecture.md](architecture.md)。

## 环境要求

- Rust 2024 edition
- Podman，或兼容 compose 的 Docker
- 本地端口可用：`5432`、`9092`、`8123`、`9000`、`8080`、`9100`、`9101`、`9102`

## 本地基础设施

启动：

```bash
./scripts/dev-up
```

查看状态：

```bash
podman compose ps
```

停止：

```bash
./scripts/dev-down
```

`scripts/dev-up` 使用 `compose.yml` 和 `compose.override.yml`，会启动：

| 服务 | 端口 | 用途 |
|------|------|------|
| PostgreSQL | `5432` | rooms 和 worker leases |
| Redpanda | `9092` | Kafka API |
| ClickHouse | `8123`、`9000` | HTTP 和 native 端口 |

## 构建和测试

完整检查：

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

测试依赖：

| 命令 | 依赖 |
|------|------|
| `cargo fmt --check` | 无外部服务 |
| `cargo clippy --all-targets -- -D warnings` | 无外部服务 |
| `cargo test --lib` | PostgreSQL，registry 测试会连接本地库 |
| `cargo test --all-targets` | PostgreSQL、Redpanda、ClickHouse |
| `cargo test --test multi_room_stability` | Redpanda、ClickHouse |

## 启动服务

三个进程通常分别运行：

```bash
cargo run --bin writer
cargo run --bin api
cargo run --bin collector
```

推荐先启动 `writer`，再启动 `api` 和 `collector`。`writer` 和 `collector` 都会确保 Redpanda topics 存在；`writer` 还会确保 ClickHouse 表存在。

默认配置文件是 `config/default.toml`。可用 `-c` 指定配置：

```bash
cargo run --bin writer -- -c config/default.toml
```

## 房间管理

添加房间：

```bash
curl -X POST http://localhost:8080/rooms \
  -H 'Content-Type: application/json' \
  -d '{"room_id": 21484828}'
```

列出房间：

```bash
curl http://localhost:8080/rooms
```

禁用房间：

```bash
curl -X PUT http://localhost:8080/rooms/21484828/disable
```

启用房间：

```bash
curl -X PUT http://localhost:8080/rooms/21484828/enable
```

列出租约：

```bash
curl http://localhost:8080/leases
```

禁用房间会把 `rooms.enabled` 设为 false，并删除该房间当前 lease。collector 会在下一次房间状态检查时停止对应房间任务。

## Collector 运行模式

默认 registry 模式：

```bash
cargo run --bin collector -- --capacity 100
```

行为：

- 从 PostgreSQL 认领 enabled rooms
- 每个认领房间启动一个 room task
- 每 30 秒续租
- 房间 disabled、lease lost 或超过重试上限时退出 room task

单房间模式：

```bash
cargo run --bin collector -- --room-id 21484828
```

该模式不使用 PostgreSQL registry，但仍需要 Redpanda。

只检查 Bilibili 鉴权：

```bash
cargo run --bin collector -- --check-live-auth 21484828
```

仅租约模式：

```bash
cargo run --bin collector -- --lease-only --capacity 10
```

该模式只认领和续租，不连接 Bilibili，适合检查 registry 调度。

## Prometheus 指标

每个二进制在自己的 `metrics_addr` 暴露 `/metrics`：

| 进程 | 地址 |
|------|------|
| collector | `http://localhost:9100/metrics` |
| writer | `http://localhost:9101/metrics` |
| api | `http://localhost:9102/metrics` |

快速检查：

```bash
curl http://localhost:8080/health
curl http://localhost:9100/metrics
curl http://localhost:9101/metrics
curl http://localhost:9102/metrics
```

关键指标：

| 指标 | 判断 |
|------|------|
| `bilive_active_rooms` | 当前 collector 正在连接的房间数 |
| `bilive_events_total` | collector 已解析并发布的事件数 |
| `bilive_publish_errors_total` | 发布到 Redpanda 失败次数 |
| `bilive_parser_errors_total` | Bilibili 消息解析失败次数 |
| `bilive_reconnects_total` | 房间连接重试次数 |
| `bilive_inserts_total` | ClickHouse insert 成功次数 |
| `bilive_commit_errors_total` | Redpanda offset commit 失败次数 |
| `bilive_insert_latency_seconds` | ClickHouse insert 延迟 |
| `bilive_consumer_lag` | broker high watermark 与 committed offset 的差值，按 topic 聚合 |
| `bilive_bad_messages_total` | writer 无法反序列化的消息数 |

## ClickHouse 查询

弹幕计数：

```bash
curl -G http://localhost:8123/ \
  --data-urlencode 'query=SELECT room_id, count() FROM bilibili_live_danmaku GROUP BY room_id ORDER BY count() DESC LIMIT 10'
```

礼物计数：

```bash
curl -G http://localhost:8123/ \
  --data-urlencode 'query=SELECT room_id, count() FROM bilibili_live_gifts GROUP BY room_id ORDER BY count() DESC LIMIT 10'
```

最近弹幕：

```bash
curl -G http://localhost:8123/ \
  --data-urlencode 'query=SELECT room_id, uname, message, source_partition, source_offset FROM bilibili_live_danmaku ORDER BY received_at DESC LIMIT 10'
```

清空表需要 POST body。推荐：

```bash
curl -sS http://localhost:8123/ \
  --data-binary 'TRUNCATE TABLE bilibili_live_danmaku'

curl -sS http://localhost:8123/ \
  --data-binary 'TRUNCATE TABLE bilibili_live_gifts'
```

## 实际验收流程

以下流程用于确认真实采集和写入。

1. 启动 infra：

```bash
./scripts/dev-up
```

2. 启动 `writer`、`api`、`collector`：

```bash
cargo run --bin writer
cargo run --bin api
cargo run --bin collector -- --capacity 2
```

3. 添加房间：

```bash
curl -X POST http://localhost:8080/rooms \
  -H 'Content-Type: application/json' \
  -d '{"room_id": 6154037}'

curl -X POST http://localhost:8080/rooms \
  -H 'Content-Type: application/json' \
  -d '{"room_id": 23438368}'
```

4. 确认认领和连接：

```bash
curl http://localhost:8080/rooms
curl http://localhost:8080/leases
curl http://localhost:9100/metrics | rg 'bilive_active_rooms|bilive_events_total|bilive_reconnects_total|bilive_parser_errors_total|bilive_publish_errors_total'
```

5. 确认 ClickHouse 写入：

```bash
curl -G http://localhost:8123/ \
  --data-urlencode "query=SELECT 'danmaku' AS table, room_id, count() FROM bilibili_live_danmaku WHERE room_id IN (6154037,23438368) GROUP BY room_id UNION ALL SELECT 'gifts' AS table, room_id, count() FROM bilibili_live_gifts WHERE room_id IN (6154037,23438368) GROUP BY room_id ORDER BY table, room_id"
```

6. 确认 writer lag：

```bash
curl http://localhost:9101/metrics | rg 'bilive_consumer_lag|bilive_commit_errors_total|bilive_inserts_total'
```

7. 禁用房间并确认停止：

```bash
curl -X PUT http://localhost:8080/rooms/6154037/disable
curl -X PUT http://localhost:8080/rooms/23438368/disable
curl http://localhost:8080/leases
curl http://localhost:9100/metrics | rg 'bilive_active_rooms'
```

验收通过标准：

- `/leases` 中能看到 enabled room 的有效租约
- `/rooms` 中目标房间出现 `last_connected_at`，且 `last_error` 为 null 或可解释
- `bilive_events_total` 增长
- ClickHouse 对应房间有 danmaku 或 gift 记录
- `bilive_consumer_lag` 最终回落或保持稳定
- disable 后 lease 消失，`bilive_active_rooms` 降低

## 排障

### API 无法访问

检查：

```bash
ss -ltnp | rg ':8080'
curl http://localhost:8080/health
```

如果 API 启动失败，优先看 PostgreSQL 是否运行，以及 `config/default.toml` 中 `postgres.url` 是否正确。

### Collector 没有认领房间

检查：

```bash
curl http://localhost:8080/rooms
curl http://localhost:8080/leases
```

确认房间 `enabled=true`，collector `--capacity` 大于 0，且没有其他 worker 持有未过期 lease。

### Collector 连接失败或频繁重连

检查：

```bash
cargo run --bin collector -- --check-live-auth <room_id>
curl http://localhost:9100/metrics | rg 'bilive_reconnects_total|bilive_parser_errors_total|bilive_publish_errors_total'
```

常见原因：

- Bilibili API 或 WebSocket endpoint 网络不可达
- 房间不存在或不可访问
- Redpanda 发布失败
- Bilibili 返回了当前解析器不支持或畸形的消息

### Writer 没有写入 ClickHouse

检查：

```bash
curl http://localhost:9101/metrics | rg 'bilive_inserts_total|bilive_commit_errors_total|bilive_consumer_lag|bilive_bad_messages_total'
curl -G http://localhost:8123/ --data-urlencode 'query=SHOW TABLES'
```

常见原因：

- ClickHouse 不可达
- Redpanda 没有事件
- writer lag 持续增长，说明 writer 跟不上或 commit 失败
- bad message 增长，说明 Redpanda 中有无法反序列化的 payload

### ClickHouse 重复记录

writer 是 at-least-once。若 ClickHouse insert 成功后，offset commit 失败并且进程崩溃，重启后 Redpanda 可能重放，ClickHouse 可能出现重复记录。用 `source_topic`、`source_partition`、`source_offset` 可以定位重复来源。

## 配置参考

`config/default.toml`：

| 节 | 字段 | 默认值 | 说明 |
|----|------|--------|------|
| log | level | info | 日志级别 |
| collector | metrics_addr | 0.0.0.0:9100 | collector metrics 地址 |
| collector | startup_delay_ms | 200 | 启动房间任务之间的延迟 |
| collector | api_concurrency_limit | 10 | Bilibili API 并发上限 |
| collector | endpoint_rate_limit | 20 | WebSocket 建连并发上限 |
| collector | reconnect_base_ms | 1000 | 指数退避基准 |
| collector | reconnect_max_ms | 60000 | 指数退避上限 |
| collector | reconnect_max_retries | 10 | 单次 room task 最大重试次数 |
| writer | metrics_addr | 0.0.0.0:9101 | writer metrics 地址 |
| writer | batch_size | 100 | 每批 ClickHouse 写入行数 |
| writer | batch_timeout_ms | 5000 | 非满批次最大等待时间 |
| api | listen_addr | 0.0.0.0:8080 | API 监听地址 |
| api | metrics_addr | 0.0.0.0:9102 | API metrics 地址 |
| postgres | url | postgres://bilive:bilive@localhost:5432/bilive | PostgreSQL URL |
| clickhouse | url | http://localhost:8123 | ClickHouse HTTP URL |
| redpanda | bootstrap_servers | localhost:9092 | Kafka bootstrap 地址 |
