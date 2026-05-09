# Plan: Bilibili Live Event Recorder

## Goal

Build a standalone Rust service for recording Bilibili live-room danmaku and gift events from many rooms.

This is a recording pipeline. Design around collection, scheduling, backpressure, normalized storage, and operational visibility.

The first milestone is a correct, stable, observable pipeline for a modest number of rooms. The architecture should not block future scale.

## Philosophy

Favor domain adequacy: keep what the domain requires, remove what the domain does not justify.

Build the smallest complete recording pipeline whose boundaries are already correct for future scale.

Every layer should have a clear responsibility. Complexity must be justified by scale, correctness, or clarity.

## Stack

Use:

```text
Rust + Tokio
WebSocket live transport
Redpanda
ClickHouse
PostgreSQL
tracing + metrics + Prometheus
Podman Compose
````

Use Podman-compatible local infrastructure.

Preferred commands:

```bash
podman compose up -d
podman compose down
podman compose logs -f
podman compose config
```

Prefer neutral local-dev names such as:

```text
compose.yml
compose.override.yml
scripts/dev-up
scripts/dev-down
```

## Local Reference

A previous project exists at:

```text
.temp/bilive-coyote
```

Use it only as a read-only reference for Bilibili live-room message behavior:

```text
room auth
live WebSocket connection
heartbeat
packet decoding
compression handling
JSON extraction
event parsing
```

The new project is a fresh recording pipeline. Its public names should be based on the new domain.

Use naming based on:

```text
live
live_message
danmaku
gift
room_event
```

Examples:

```text
LiveSource
LiveTransport
LiveEvent
LiveAuth
LiveEndpoint
RoomTask
DanmakuEvent
GiftEvent
bilibili.live.danmaku.v1
bilibili.live.gift.v1
bilibili_live_danmaku
bilibili_live_gifts
```

## System Shape

```text
Bilibili control-plane API
        │
        ▼
room auth/cache
        │
        ▼
WebSocket live transport
        │
        ▼
typed normalized live events
        │
        ▼
Redpanda
        │
        ▼
batch writer
        │
        ▼
ClickHouse
```

PostgreSQL owns rooms, worker state, leases, and metadata.

The Bilibili API is the low-frequency control plane. The live WebSocket connection is the high-frequency data plane.

## Core Components

### Protocol

Own Bilibili live packet behavior and event parsing.

Responsibilities:

* build auth and heartbeat packets
* decode incoming packets
* handle compression
* extract JSON messages
* parse supported live message types into typed records
* classify unsupported or malformed messages without crashing collectors

Start with danmaku and gifts. Add more live event types when their storage shape is clear.

Persist normalized records with diagnostic metadata such as command type and parser version.

### Control Plane

Own low-frequency Bilibili API interaction.

Responsibilities:

* resolve room identifiers
* fetch identity/session material required for live connection
* fetch live server endpoints and auth token
* cache control-plane results
* refresh auth when expired or invalid

Ordinary reconnects should reuse existing auth and endpoints first.

### Collector

Own room lifecycle.

Responsibilities:

* claim rooms from PostgreSQL
* keep leases alive
* start one room lifecycle per claimed room
* connect to live WebSocket endpoint
* send auth and heartbeat
* read packets continuously
* parse live events
* publish typed events to Redpanda
* report room status and metrics
* reconnect with backoff and jitter

Room lifecycle should distinguish:

* network failure
* endpoint failure
* auth failure
* room unavailable
* permission/session failure
* parser failure
* output queue failure

Failure handling should be observable and rate-limited.

### Scheduler / Leases

Use PostgreSQL for room registry and worker leases.

Responsibilities:

* store enabled rooms
* track which worker owns which room
* expire stale leases
* allow takeover of expired leases
* expose last connected time and last error

Keep scheduling simple and explicit.

### Redpanda

Use Redpanda as the event buffer between collectors and writers.

Responsibilities:

* absorb bursts
* decouple collection from storage
* provide short-term replay
* allow collector and writer horizontal scaling

Partition by room identity.

Use live-event-oriented topics:

```text
bilibili.live.danmaku.v1
bilibili.live.gift.v1
bilibili.live.room_status.v1
```

### Writer

Own durable event insertion into ClickHouse.

Responsibilities:

* consume events from Redpanda
* validate event shape
* batch records
* insert into ClickHouse
* commit queue offsets after successful insertion
* retry transient failures
* expose write metrics

Writes should be batched.

### ClickHouse

Own normalized queryable history.

Store danmaku and gift records in query-friendly tables.

Records should support common queries:

* room timeline
* user messages in a room
* gift history by room
* gift ranking by room/time
* event volume over time
* collector/parser diagnostics by time range

Suggested table names:

```text
bilibili_live_danmaku
bilibili_live_gifts
bilibili_live_room_status
```

### API

Provide minimal management and visibility.

Responsibilities:

* health check
* metrics
* add/list/update rooms
* enable/disable rooms
* inspect workers
* inspect leases and room status

## Acceptance Policy

The Agent must clearly distinguish automatic validation from user-gated validation.

### Automatic Acceptance

Run and report automatic checks whenever possible:

```text
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo check
podman compose config
local service startup checks
unit tests
integration tests using local Redpanda/PostgreSQL/ClickHouse
mocked protocol/parser tests
mocked control-plane tests
```

Automatic acceptance may use local containers, mocks, fixtures, and deterministic test data.

### User-Gated Acceptance

Stop and ask the user before claiming success for checks that require:

```text
real Bilibili API access
real Bilibili room connection
cookies/login/session credentials
large room lists
long-running tests
external network reliability
24-hour stability runs
100+ room live tests
production-like Redpanda/ClickHouse tuning
```

The Agent may prepare commands and scripts for these checks, but must not claim they passed unless the user runs them or explicitly authorizes the run.

### Reporting Rule

For every phase, report:

```text
Implemented:
Auto-verified:
Needs user validation:
Known limitations:
Next step:
```

Use “implementation complete; live validation pending” when user-gated validation remains.

## Operational Requirements

The system should provide:

* bounded internal queues
* reconnect backoff
* reconnect jitter
* startup throttling
* global control-plane rate limiting
* endpoint connection limiting
* room-level status
* collector metrics
* writer metrics
* parser error metrics
* Redpanda lag visibility
* ClickHouse insert visibility

Important event loss must be visible through metrics and logs.

## Implementation Phases

### Phase 1: Skeleton

Create the project, fixed services, configuration, logging, metrics endpoints, and Podman Compose local infrastructure.

Auto Acceptance:

```text
cargo fmt --check passes
cargo clippy --all-targets -- -D warnings passes
cargo test passes
podman compose config passes
PostgreSQL starts locally
Redpanda starts locally
ClickHouse starts locally
collector binary starts
writer binary starts
api binary starts
metrics endpoint responds locally
```

User-Gated Acceptance:

```text
none
```

### Phase 2: Protocol and Parser

Implement live packet handling and typed parsing for danmaku and gift events.

Use `.temp/bilive-coyote` as reference for protocol behavior.

Auto Acceptance:

```text
auth packet test passes
heartbeat packet test passes
packet decode tests pass
compression tests pass using fixtures
JSON extraction tests pass
danmaku fixture parses into typed event
gift fixture parses into typed event
malformed JSON does not panic
unsupported command is classified without panic
public names use live/danmaku/gift/room-event terminology
```

User-Gated Acceptance:

```text
none
```

### Phase 3: Control Plane

Implement the minimum Bilibili API flow needed to connect to a room, plus caching.

Auto Acceptance:

```text
room auth cache tests pass
expired cache refresh behavior is tested with mocks
network error classification is tested with mocks
control-plane client compiles and has clear integration boundaries
public names use live/danmaku/gift/room-event terminology
```

User-Gated Acceptance:

```text
real room id resolves through Bilibili API
real live auth returns WebSocket endpoints and auth token
login/session behavior works if credentials are required
```

Provide a command for the user to run with a real room id.

### Phase 4: Single Room Pipeline

Connect one room end-to-end.

Auto Acceptance:

```text
collector can publish synthetic typed events to Redpanda
writer consumes synthetic events from Redpanda
ClickHouse receives synthetic danmaku records
ClickHouse receives synthetic gift records
writer batches inserts
offset/ack behavior is tested locally
metrics are exposed
```

User-Gated Acceptance:

```text
collector connects to one real Bilibili room
heartbeat works against real live server
real danmaku/gift events reach Redpanda
real danmaku/gift events reach ClickHouse
disconnect triggers controlled reconnect in real network conditions
```

Provide commands for the user to run with a real room id.

### Phase 5: Room Registry and Leases

Introduce PostgreSQL rooms and worker leases.

Auto Acceptance:

```text
rooms can be inserted locally
collector claims enabled rooms up to capacity
leases renew while worker is healthy
disabled rooms are released
expired leases can be claimed by another worker in tests
worker heartbeat updates correctly
```

User-Gated Acceptance:

```text
multi-process lease behavior with two real collector processes
manual disable/enable behavior observed by user
```

Clearly report whether multi-process checks actually ran.

### Phase 6: Multi-Room Local Stability

Run a realistic local or mocked multi-room workload.

Auto Acceptance:

```text
mocked room tasks run concurrently
bounded queues remain bounded
no obvious task leaks in test duration
writer keeps up with synthetic burst
Redpanda lag remains bounded under synthetic load
ClickHouse writes remain batched
metrics show active rooms, events, parser errors, and writer throughput
```

User-Gated Acceptance:

```text
100 real rooms run for 24 hours
no task leaks during real run
no unbounded memory growth during real run
no API hot-looping during real run
Redpanda lag remains bounded during real run
ClickHouse records are queryable after real run
room-level status remains visible during real run
```

Do not mark the 24-hour real run complete until the user confirms it.

### Phase 7: Scale Hardening

Improve scaling based on observed bottlenecks.

Auto Acceptance:

```text
startup throttling is covered by tests
global control-plane limiter is covered by tests
endpoint connection limiter is covered by tests
reconnect jitter/backoff is covered by tests
writer batch settings are configurable
metrics expose API rate, reconnect rate, queue lag, and insert latency
```

User-Gated Acceptance:

```text
real reconnect storm behavior
real API rate behavior
real hot-room skew
real collector memory per room
real writer throughput under production-like load
```

Ask for measured data before major optimization work.

## Design Rules

Prefer ownership over shared mutable state.

Prefer typed events over dynamic JSON inside the system.

Prefer explicit lifecycle states over scattered booleans.

Prefer bounded queues.

Prefer measured optimization.

Prefer small, reviewable changes.

Use domain names based on live rooms, danmaku, gifts, and room events.

## First Real Milestone

The first real milestone is:

```text
100 rooms
24 hours
queryable danmaku and gift records
no task leaks
no API hot-looping
no silent write loss
bounded Redpanda lag
visible room-level status
```

This milestone is user-gated.

The Agent may implement tooling needed to run it, but must not mark it complete until the user confirms the real run.

## Final Guiding Sentence

Keep the control plane low-frequency, the data plane long-lived, the event model typed, the queue explicit, the storage normalized, the naming domain-based, and every piece of complexity justified by scale, correctness, or clarity.
