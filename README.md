# bilive-sentinel

Bilibili 直播间弹幕和礼物录制器。它连接直播间 WebSocket，解析 `DANMU_MSG` 和 `SEND_GIFT`，经 Redpanda 缓冲后批量写入 ClickHouse。

这个项目的边界是录制管线，不是实时分析平台。它不提供查询 UI，不解析所有直播事件，不追求 exactly-once，也不在管理 API 上做认证。当前目标是一个清楚、稳定、可验收的采集和落库系统。

## 文档结构

| 文档 | 职责 |
|------|------|
| `README.md` | 项目入口、运行最短路径、当前边界 |
| [docs/architecture.md](docs/architecture.md) | 系统职责、数据流、存储模型、关键语义 |
| [docs/operations.md](docs/operations.md) | 本地运行、验收、监控、排障、配置参考 |

历史实施计划已从当前项目文档中移除。

## 系统形态

```text
Bilibili Live API  ->  collector  ->  Redpanda  ->  writer  ->  ClickHouse
                         |
                         v
                    PostgreSQL
                 rooms + leases
```

三个二进制：

| 二进制 | 职责 |
|--------|------|
| `collector` | 认领房间，连接 Bilibili WebSocket，解析事件并发布到 Redpanda |
| `writer` | 从 Redpanda 消费事件，批量写入 ClickHouse，成功后提交 offset |
| `api` | 管理房间启停，查看房间和租约状态 |

本地基础设施：

| 服务 | 用途 |
|------|------|
| PostgreSQL | 房间注册表和 collector 租约 |
| Redpanda | collector 和 writer 之间的事件缓冲 |
| ClickHouse | 弹幕和礼物的查询型历史存储 |

## 快速开始

启动基础设施：

```bash
./scripts/dev-up
```

构建：

```bash
cargo build
```

分别启动三个进程：

```bash
cargo run --bin writer
cargo run --bin api
cargo run --bin collector
```

添加房间：

```bash
curl -X POST http://localhost:8080/rooms \
  -H 'Content-Type: application/json' \
  -d '{"room_id": 21484828}'
```

查看状态：

```bash
curl http://localhost:8080/rooms
curl http://localhost:8080/leases
```

查询 ClickHouse：

```bash
curl -G http://localhost:8123/ \
  --data-urlencode 'query=SELECT room_id, count() FROM bilibili_live_danmaku GROUP BY room_id ORDER BY count() DESC LIMIT 10'
```

停止基础设施：

```bash
./scripts/dev-down
```

## 常用命令

单房间采集，不使用 PostgreSQL registry：

```bash
cargo run --bin collector -- --room-id 21484828
```

只检查直播鉴权：

```bash
cargo run --bin collector -- --check-live-auth 21484828
```

只认领租约，不连接 Bilibili：

```bash
cargo run --bin collector -- --lease-only --capacity 10
```

使用自定义配置：

```bash
cargo run --bin collector -- -c config/default.toml
```

## 测试

本仓库的部分单元测试会连接本地 PostgreSQL，完整测试还需要 Redpanda 和 ClickHouse。先启动本地基础设施：

```bash
./scripts/dev-up
```

然后运行：

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

## 当前接受的限制

| 限制 | 说明 |
|------|------|
| registry 测试依赖本地 PostgreSQL | `src/registry` 测试直接连接 `localhost:5432`。这是测试环境边界，不影响运行时语义。 |
| API 没有认证 | 当前适合本地开发或受保护内网。公网或生产部署前需要放到受保护网络内，或增加鉴权。 |

更多系统语义见 [docs/architecture.md](docs/architecture.md)，实际验收和排障步骤见 [docs/operations.md](docs/operations.md)。
