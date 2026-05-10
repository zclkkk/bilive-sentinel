# 运维指南

## 运行

### 本地开发

```bash
# 启动基础设施
./scripts/dev-up

# 构建并运行
cargo build
cargo run --bin writer &
cargo run --bin collector &
cargo run --bin api &

# 添加房间
curl -X POST http://localhost:8080/rooms \
  -H 'Content-Type: application/json' \
  -d '{"room_id": 21484828}'

# 查看日志
# tracing 默认输出到 stderr
```

### 单房间模式

测试特定房间，不需要 PostgreSQL：

```bash
cargo run --bin collector -- --room-id 21484828
```

从 Bilibili 获取鉴权信息，连接直播间，发布到 Redpanda。需要 Redpanda 运行。

### 仅租约模式

测试房间注册表，不连接 Bilibili：

```bash
# 先通过 API 添加房间
curl -X POST http://localhost:8080/rooms \
  -H 'Content-Type: application/json' \
  -d '{"room_id": 12345}'

# 认领租约但不连接
cargo run --bin collector -- --lease-only --capacity 10
```

### 查看鉴权信息

打印房间鉴权信息，不建立连接：

```bash
cargo run --bin collector -- --check-live-auth 21484828
```

## 监控

### Prometheus 指标

每个二进制在配置的 `metrics_addr` 上暴露指标：

- Collector: `http://localhost:9100/metrics`
- Writer: `http://localhost:9101/metrics`
- API: `http://localhost:9102/metrics`

### 关键指标

**Collector 健康：**
- `bilive_active_rooms` — 应与预期房间数一致
- `bilive_reconnects_total` — 速率过高表示网络或鉴权问题
- `bilive_parser_errors_total` — 非零表示收到 Bilibili 的畸形消息

**Writer 健康：**
- `bilive_insert_latency_seconds` — p99 应低于 1 秒（本地 ClickHouse）
- `bilive_consumer_lag` — 持续增长表示 writer 处理速度不足
- `bilive_inserts_total` — 应稳定增长

**全局：**
- `bilive_sentinel_up` — 1 表示服务运行中

### 健康检查

```bash
curl http://localhost:8080/health
# 返回: ok
```

## 房间管理

### 添加房间

```bash
curl -X POST http://localhost:8080/rooms \
  -H 'Content-Type: application/json' \
  -d '{"room_id": 21484828}'
```

### 列出房间

```bash
curl http://localhost:8080/rooms
```

### 启用/禁用房间

```bash
# 禁用（collector 不再认领该房间）
curl -X PUT http://localhost:8080/rooms/21484828/disable

# 启用
curl -X PUT http://localhost:8080/rooms/21484828/enable
```

禁用房间会阻止新的认领。不会中断已连接的房间；房间会在下次重连时因租约过期而停止。

### 列出租约

```bash
curl http://localhost:8080/leases
```

## 故障排查

### Collector 无法连接

1. 检查房间是否已添加：`curl http://localhost:8080/rooms`
2. 检查房间是否已启用
3. 查看 collector 日志中的鉴权错误
4. 用 `--check-live-auth <room_id>` 验证 Bilibili API 访问

### Writer 延迟增长

1. 检查 `bilive_insert_latency_seconds` — ClickHouse 写入是否过慢
2. 增大配置中的 `batch_size`（每批写入更多记录）
3. 检查 ClickHouse 服务健康状况

### 重连风暴

1. 检查 `bilive_reconnects_total` 速率
2. 增大 `reconnect_base_ms` 和 `reconnect_max_ms` 放慢重试
3. 检查到 Bilibili WebSocket endpoint 的网络连通性

### 消费延迟

`bilive_consumer_lag` 报告 consumer 当前 position 与已提交 offset 的差值（按 topic）。延迟持续增长表示 writer 处理速度跟不上。

可能原因：
- ClickHouse 写入延迟过高
- batch_size 太小（频繁小批量写入）
- writer 到 ClickHouse 之间网络问题

## 配置参考

`config/default.toml` 所有字段：

| 节 | 字段 | 默认值 | 说明 |
|----|------|--------|------|
| log | level | info | 日志级别（trace/debug/info/warn/error） |
| collector | metrics_addr | 0.0.0.0:9100 | Prometheus 指标监听地址 |
| collector | startup_delay_ms | 200 | 启动时房间任务之间的延迟 |
| collector | api_concurrency_limit | 10 | Bilibili API 最大并发数 |
| collector | endpoint_rate_limit | 20 | WebSocket 最大并发建连数 |
| collector | reconnect_base_ms | 1000 | 指数退避基准延迟 |
| collector | reconnect_max_ms | 60000 | 退避最大延迟 |
| collector | reconnect_max_retries | 10 | 超过此次数后放弃重连 |
| writer | metrics_addr | 0.0.0.0:9101 | Prometheus 指标监听地址 |
| writer | batch_size | 100 | 每批 ClickHouse 写入条数 |
| writer | batch_timeout_ms | 5000 | 部分批次最大等待毫秒 |
| api | listen_addr | 0.0.0.0:8080 | API 服务监听地址 |
| api | metrics_addr | 0.0.0.0:9102 | Prometheus 指标监听地址 |
| postgres | url | postgres://bilive:bilive@localhost:5432/bilive | PostgreSQL 连接 URL |
| clickhouse | url | http://localhost:8123 | ClickHouse HTTP URL |
| redpanda | bootstrap_servers | localhost:9092 | Redpanda/Kafka bootstrap 地址 |
