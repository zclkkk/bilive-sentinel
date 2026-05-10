use anyhow::Result;
use bilive_sentinel::live_api::{LiveApi, LiveApiClient, LiveAuth};
use bilive_sentinel::protocol::{self, LiveEvent, OP_AUTH, OP_HEARTBEAT, ParsedPacket};
use bilive_sentinel::redpanda::RedpandaProducer;
use bilive_sentinel::registry::{RoomRunState, WorkerLease};
use bilive_sentinel::{Config, init_tracing, new_service_registry, start_metrics_server};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "config/default.toml")]
    config: String,

    #[arg(long)]
    room_id: Option<u64>,

    #[arg(long)]
    check_live_auth: Option<u64>,

    #[arg(long, default_value = "100")]
    capacity: usize,

    #[arg(long)]
    lease_only: bool,
}

#[derive(Clone, Copy)]
struct RoomTaskSettings {
    reconnect_base_ms: u64,
    reconnect_max_ms: u64,
    reconnect_max_retries: u32,
}

#[derive(Clone)]
struct CollectorContext {
    pool: PgPool,
    worker_id: String,
    producer: RedpandaProducer,
    client: LiveApiClient,
    endpoint_semaphore: Arc<Semaphore>,
    metrics: bilive_sentinel::metrics::CollectorMetrics,
    settings: RoomTaskSettings,
}

#[derive(Clone)]
struct RoomStatusReporter {
    pool: PgPool,
    room_id: i64,
}

struct ActiveRoomGuard {
    gauge: prometheus::Gauge,
}

impl ActiveRoomGuard {
    fn new(gauge: prometheus::Gauge) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for ActiveRoomGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(room_id) = cli.check_live_auth {
        return check_live_auth(room_id).await;
    }

    let config = Config::load(&cli.config)?;
    init_tracing(&config.log.level);

    tracing::info!("collector starting");

    let registry = new_service_registry();
    let collector_metrics = bilive_sentinel::metrics::CollectorMetrics::register(&registry);
    let metrics_addr = config.collector.metrics_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(&metrics_addr, registry).await {
            tracing::error!(error = %e, "metrics server failed");
        }
    });

    if cli.lease_only {
        return run_lease_only(&config, cli.capacity).await;
    }

    bilive_sentinel::redpanda::ensure_topics(&config.redpanda.bootstrap_servers)
        .await
        .map_err(|e| anyhow::anyhow!("ensure_topics: {e}"))?;

    if let Some(room_id) = cli.room_id {
        run_single_room(&config, room_id, collector_metrics).await
    } else {
        run_registry_mode(&config, cli.capacity, collector_metrics).await
    }
}

async fn run_lease_only(config: &Config, capacity: usize) -> Result<()> {
    let pool = sqlx::PgPool::connect(&config.postgres.url)
        .await
        .map_err(|e| anyhow::anyhow!("postgres connect: {e}"))?;

    bilive_sentinel::registry::create_tables(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("create_tables: {e}"))?;

    let worker_id = uuid::Uuid::new_v4().to_string();
    let lease_ttl = chrono::Duration::seconds(60);

    tracing::info!(worker_id, capacity, "claiming rooms (lease-only mode)");
    let leases =
        bilive_sentinel::registry::claim_available_rooms(&pool, &worker_id, capacity, lease_ttl)
            .await
            .map_err(|e| anyhow::anyhow!("claim_available_rooms: {e}"))?;

    for lease in &leases {
        tracing::info!(
            room_id = lease.room_id,
            worker_id = lease.worker_id,
            "claimed room"
        );
    }
    tracing::info!(count = leases.len(), "total rooms claimed");

    // Renewal loop
    let pool_clone = pool.clone();
    let worker_id_clone = worker_id.clone();
    let renewal_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            let leases = bilive_sentinel::registry::list_leases(&pool_clone)
                .await
                .unwrap_or_default();
            for lease in leases.iter().filter(|l| l.worker_id == worker_id_clone) {
                match bilive_sentinel::registry::renew_lease(
                    &pool_clone,
                    lease.room_id,
                    &worker_id_clone,
                    chrono::Duration::seconds(60),
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::info!(room_id = lease.room_id, "lease no longer renewable");
                    }
                    Err(e) => {
                        tracing::warn!(room_id = lease.room_id, error = %e, "renew lease failed")
                    }
                }
            }
        }
    });

    tokio::signal::ctrl_c().await?;

    tracing::info!("shutting down, releasing leases");
    renewal_handle.abort();
    bilive_sentinel::registry::release_all_leases(&pool, &worker_id)
        .await
        .map_err(|e| anyhow::anyhow!("release_all_leases: {e}"))?;

    tracing::info!("collector shutting down (lease-only mode)");
    Ok(())
}

async fn run_single_room(
    config: &Config,
    room_id: u64,
    metrics: bilive_sentinel::metrics::CollectorMetrics,
) -> Result<()> {
    let producer = RedpandaProducer::new(&config.redpanda.bootstrap_servers);
    let client = LiveApiClient::new(config.collector.api_concurrency_limit);
    let ep_semaphore = Arc::new(Semaphore::new(config.collector.endpoint_rate_limit));
    let settings = RoomTaskSettings {
        reconnect_base_ms: config.collector.reconnect_base_ms,
        reconnect_max_ms: config.collector.reconnect_max_ms,
        reconnect_max_retries: config.collector.reconnect_max_retries,
    };

    let mut retries: u32 = 0;
    let mut cached_auth: Option<LiveAuth> = None;

    loop {
        let auth = match cached_auth.take() {
            Some(auth) => auth,
            None => {
                tracing::info!(room_id, "fetching live auth");
                match client.fetch_live_auth(room_id).await {
                    Ok(auth) => auth,
                    Err(e) => {
                        retries += 1;
                        metrics.reconnects_total.inc();
                        if retries > settings.reconnect_max_retries {
                            return Err(anyhow::anyhow!("fetch_live_auth: {e}"));
                        }
                        let delay = next_backoff(settings, retries);
                        tracing::warn!(
                            room_id,
                            retries,
                            delay_ms = delay.as_millis() as u64,
                            error = %e,
                            "auth failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                }
            }
        };

        tracing::info!(room_id = auth.room_id, "connecting to room");
        match run_room(&auth, &producer, &ep_semaphore, &metrics, None).await {
            Ok(()) => break,
            Err(e) => {
                retries += 1;
                metrics.reconnects_total.inc();
                if retries > settings.reconnect_max_retries {
                    return Err(anyhow::anyhow!("max retries exceeded: {e}"));
                }
                if !should_refresh_auth(&e) {
                    cached_auth = Some(auth);
                }
                let delay = next_backoff(settings, retries);
                tracing::warn!(
                    room_id,
                    retries,
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "room disconnected, reconnecting"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }

    tracing::info!("collector shutting down");
    Ok(())
}

async fn run_registry_mode(
    config: &Config,
    capacity: usize,
    metrics: bilive_sentinel::metrics::CollectorMetrics,
) -> Result<()> {
    let pool = sqlx::PgPool::connect(&config.postgres.url)
        .await
        .map_err(|e| anyhow::anyhow!("postgres connect: {e}"))?;

    bilive_sentinel::registry::create_tables(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("create_tables: {e}"))?;

    let worker_id = uuid::Uuid::new_v4().to_string();
    let lease_ttl = chrono::Duration::seconds(60);

    let producer = RedpandaProducer::new(&config.redpanda.bootstrap_servers);
    let client = LiveApiClient::new(config.collector.api_concurrency_limit);
    let endpoint_semaphore = Arc::new(Semaphore::new(config.collector.endpoint_rate_limit));
    let settings = RoomTaskSettings {
        reconnect_base_ms: config.collector.reconnect_base_ms,
        reconnect_max_ms: config.collector.reconnect_max_ms,
        reconnect_max_retries: config.collector.reconnect_max_retries,
    };
    let startup_delay = Duration::from_millis(config.collector.startup_delay_ms);

    let context = CollectorContext {
        pool: pool.clone(),
        worker_id: worker_id.clone(),
        producer,
        client,
        endpoint_semaphore,
        metrics,
        settings,
    };

    let mut join_set = tokio::task::JoinSet::new();
    let mut active_rooms = HashSet::new();

    // Spawn lease renewal task
    let pool_clone = pool.clone();
    let worker_id_clone = worker_id.clone();
    let renewal_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            let leases = bilive_sentinel::registry::list_leases(&pool_clone)
                .await
                .unwrap_or_default();
            for lease in leases.iter().filter(|l| l.worker_id == worker_id_clone) {
                match bilive_sentinel::registry::renew_lease(
                    &pool_clone,
                    lease.room_id,
                    &worker_id_clone,
                    chrono::Duration::seconds(60),
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::info!(room_id = lease.room_id, "lease no longer renewable");
                    }
                    Err(e) => {
                        tracing::warn!(room_id = lease.room_id, error = %e, "renew lease failed")
                    }
                }
            }
        }
    });

    let mut claim_interval = tokio::time::interval(Duration::from_secs(15));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            _ = claim_interval.tick() => {
                claim_and_spawn_available(
                    &context,
                    capacity,
                    lease_ttl,
                    startup_delay,
                    &mut active_rooms,
                    &mut join_set,
                )
                .await?;
            }
            joined = join_set.join_next(), if !active_rooms.is_empty() => {
                match joined {
                    Some(Ok(room_id)) => {
                        active_rooms.remove(&room_id);
                        tracing::info!(room_id, active = active_rooms.len(), "room task exited");
                    }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "room task join failed");
                    }
                    None => {}
                }
            }
        }
    }

    // Shutdown: release leases and abort tasks
    tracing::info!("shutting down, releasing leases");
    renewal_handle.abort();
    bilive_sentinel::registry::release_all_leases(&pool, &worker_id)
        .await
        .map_err(|e| anyhow::anyhow!("release_all_leases: {e}"))?;
    join_set.abort_all();

    tracing::info!("collector shutting down");
    Ok(())
}

async fn claim_and_spawn_available(
    context: &CollectorContext,
    capacity: usize,
    lease_ttl: chrono::Duration,
    startup_delay: Duration,
    active_rooms: &mut HashSet<u64>,
    join_set: &mut tokio::task::JoinSet<u64>,
) -> Result<()> {
    let remaining = capacity.saturating_sub(active_rooms.len());
    if remaining == 0 {
        return Ok(());
    }

    tracing::debug!(remaining, "claiming available rooms");
    let leases = bilive_sentinel::registry::claim_available_rooms(
        &context.pool,
        &context.worker_id,
        remaining,
        lease_ttl,
    )
    .await
    .map_err(|e| anyhow::anyhow!("claim_available_rooms: {e}"))?;

    if leases.is_empty() {
        return Ok(());
    }
    tracing::info!(count = leases.len(), "claimed rooms");

    for (i, lease) in leases.into_iter().enumerate() {
        if i > 0 {
            tokio::time::sleep(startup_delay).await;
        }
        spawn_claimed_room(context, lease, active_rooms, join_set);
    }
    Ok(())
}

fn spawn_claimed_room(
    context: &CollectorContext,
    lease: WorkerLease,
    active_rooms: &mut HashSet<u64>,
    join_set: &mut tokio::task::JoinSet<u64>,
) {
    let room_id = lease.room_id as u64;
    if !active_rooms.insert(room_id) {
        tracing::warn!(room_id, "claimed room is already active");
        return;
    }

    let context = context.clone();
    join_set.spawn(async move {
        run_claimed_room(context, room_id).await;
        room_id
    });
}

async fn run_claimed_room(context: CollectorContext, room_id: u64) {
    let room_id_i64 = room_id as i64;
    let mut retries: u32 = 0;
    let mut cached_auth: Option<LiveAuth> = None;
    let status = RoomStatusReporter {
        pool: context.pool.clone(),
        room_id: room_id_i64,
    };

    loop {
        match room_run_state_or_stop(&context, room_id_i64).await {
            Ok(()) => {}
            Err(reason) => {
                tracing::info!(room_id, reason, "stopping room task");
                break;
            }
        }

        let auth = match cached_auth.take() {
            Some(auth) => auth,
            None => match context.client.fetch_live_auth(room_id).await {
                Ok(auth) => auth,
                Err(e) => {
                    let msg = e.to_string();
                    status.mark_error(&msg).await;
                    retries += 1;
                    context.metrics.reconnects_total.inc();
                    if retries > context.settings.reconnect_max_retries {
                        tracing::warn!(
                            room_id,
                            retries,
                            error = %e,
                            "max retries exceeded after auth failure"
                        );
                        break;
                    }
                    let delay = next_backoff(context.settings, retries);
                    tracing::warn!(
                        room_id,
                        retries,
                        delay_ms = delay.as_millis() as u64,
                        error = %e,
                        "auth failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
            },
        };

        let room_result = tokio::select! {
            result = run_room(
                &auth,
                &context.producer,
                &context.endpoint_semaphore,
                &context.metrics,
                Some(&status),
            ) => RoomLoopResult::Disconnected(result),
            stop = wait_for_room_stop(context.pool.clone(), room_id_i64, context.worker_id.clone()) => {
                RoomLoopResult::Stopped(stop)
            }
        };

        match room_result {
            RoomLoopResult::Stopped(Ok(reason)) => {
                tracing::info!(room_id, reason, "room stopped by registry state");
                break;
            }
            RoomLoopResult::Stopped(Err(e)) => {
                let msg = e.to_string();
                status.mark_error(&msg).await;
                tracing::warn!(room_id, error = %e, "room state check failed");
                break;
            }
            RoomLoopResult::Disconnected(Ok(())) => {
                tracing::warn!(room_id, "room ended without error, reconnecting");
            }
            RoomLoopResult::Disconnected(Err(e)) => {
                let msg = e.to_string();
                status.mark_error(&msg).await;
                if !should_refresh_auth(&e) {
                    cached_auth = Some(auth);
                }
                tracing::warn!(room_id, error = %e, "room disconnected");
            }
        }

        retries += 1;
        context.metrics.reconnects_total.inc();
        if retries > context.settings.reconnect_max_retries {
            tracing::warn!(room_id, retries, "max retries exceeded, giving up");
            break;
        }
        let delay = next_backoff(context.settings, retries);
        tracing::warn!(
            room_id,
            retries,
            delay_ms = delay.as_millis() as u64,
            "reconnecting room"
        );
        tokio::time::sleep(delay).await;
    }

    if let Err(e) =
        bilive_sentinel::registry::release_lease(&context.pool, room_id_i64, &context.worker_id)
            .await
    {
        tracing::warn!(room_id, error = %e, "release lease failed");
    }
}

enum RoomLoopResult {
    Disconnected(std::result::Result<(), RoomError>),
    Stopped(Result<String>),
}

async fn room_run_state_or_stop(
    context: &CollectorContext,
    room_id: i64,
) -> std::result::Result<(), String> {
    match bilive_sentinel::registry::room_run_state(&context.pool, room_id, &context.worker_id)
        .await
        .map_err(|e| e.to_string())?
    {
        RoomRunState::Runnable => Ok(()),
        RoomRunState::Disabled => Err("room disabled".into()),
        RoomRunState::LeaseLost => Err("lease lost".into()),
    }
}

async fn wait_for_room_stop(pool: PgPool, room_id: i64, worker_id: String) -> Result<String> {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        match bilive_sentinel::registry::room_run_state(&pool, room_id, &worker_id).await? {
            RoomRunState::Runnable => {}
            RoomRunState::Disabled => return Ok("room disabled".into()),
            RoomRunState::LeaseLost => return Ok("lease lost".into()),
        }
    }
}

impl RoomStatusReporter {
    async fn mark_connected(&self) {
        if let Err(e) =
            bilive_sentinel::registry::mark_room_connected(&self.pool, self.room_id).await
        {
            tracing::warn!(
                room_id = self.room_id,
                error = %e,
                "failed to update room connected status"
            );
        }
    }

    async fn mark_error(&self, error: &str) {
        let error = truncate_status_error(error);
        if let Err(e) =
            bilive_sentinel::registry::mark_room_error(&self.pool, self.room_id, &error).await
        {
            tracing::warn!(
                room_id = self.room_id,
                error = %e,
                "failed to update room error status"
            );
        }
    }
}

fn truncate_status_error(error: &str) -> String {
    const MAX_ERROR_LEN: usize = 512;
    error.chars().take(MAX_ERROR_LEN).collect()
}

fn next_backoff(settings: RoomTaskSettings, retries: u32) -> Duration {
    bilive_sentinel::backoff::calculate_backoff(
        settings.reconnect_base_ms,
        settings.reconnect_max_ms,
        retries,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    )
}

async fn check_live_auth(room_id: u64) -> Result<()> {
    let client = bilive_sentinel::live_api::LiveApiClient::default();
    match client.fetch_live_auth(room_id).await {
        Ok(auth) => {
            println!("Auth info for room {room_id}:");
            println!("  Room ID: {}", auth.room_id);
            println!("  UID: {:?}", auth.uid);
            println!("  Token: {}", auth.token);
            println!("  Buvid3: {}", auth.buvid3);
            println!("  Endpoints:");
            for ep in &auth.endpoints {
                println!("    {}:{}", ep.host, ep.port);
            }
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
    Ok(())
}

async fn run_room(
    auth: &LiveAuth,
    producer: &RedpandaProducer,
    endpoint_semaphore: &Arc<Semaphore>,
    metrics: &bilive_sentinel::metrics::CollectorMetrics,
    status: Option<&RoomStatusReporter>,
) -> std::result::Result<(), RoomError> {
    let _active_room = ActiveRoomGuard::new(metrics.active_rooms.clone());
    run_room_inner(auth, producer, endpoint_semaphore, metrics, status).await
}

async fn run_room_inner(
    auth: &LiveAuth,
    producer: &RedpandaProducer,
    endpoint_semaphore: &Arc<Semaphore>,
    metrics: &bilive_sentinel::metrics::CollectorMetrics,
    status: Option<&RoomStatusReporter>,
) -> std::result::Result<(), RoomError> {
    const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

    if auth.endpoints.is_empty() {
        return Err(RoomError::NoEndpoint);
    }

    let mut ws_stream = None;
    let mut last_error = None;

    for endpoint in &auth.endpoints {
        let url = format!("wss://{}:{}/sub", endpoint.host, endpoint.port);
        tracing::info!(url, "trying endpoint");

        let ep_permit = endpoint_semaphore
            .acquire()
            .await
            .map_err(|_| RoomError::Endpoint("endpoint limiter closed".into()))?;

        let result = tokio::time::timeout(WS_CONNECT_TIMEOUT, connect_async(&url)).await;
        drop(ep_permit);

        match result {
            Ok(Ok((stream, _))) => {
                tracing::info!(url, "connected");
                ws_stream = Some(stream);
                break;
            }
            Ok(Err(e)) => {
                tracing::warn!(url, error = %e, "endpoint connection failed");
                last_error = Some(RoomError::Endpoint(e.to_string()));
            }
            Err(_) => {
                tracing::warn!(url, "endpoint connection timed out");
                last_error = Some(RoomError::Endpoint("websocket connect timed out".into()));
            }
        }
    }

    let Some(ws_stream) = ws_stream else {
        return Err(last_error.unwrap_or(RoomError::Endpoint("all endpoints failed".into())));
    };

    let (mut write, mut read) = ws_stream.split();

    // Send auth
    let auth_body = serde_json::json!({
        "uid": auth.uid.unwrap_or(0),
        "roomid": auth.room_id,
        "protover": 3,
        "platform": "web",
        "type": 2,
        "key": auth.token,
        "buvid": auth.buvid3,
    });
    let auth_packet = protocol::build_packet(OP_AUTH, &auth_body.to_string());
    write
        .send(Message::Binary(auth_packet.into()))
        .await
        .map_err(|e| RoomError::Network(e.to_string()))?;
    tracing::info!("auth sent");

    let room_id = auth.room_id;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(20));

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let packet = protocol::build_packet(OP_HEARTBEAT, "");
                write.send(Message::Binary(packet.into())).await
                    .map_err(|e| RoomError::Network(e.to_string()))?;
            }
            msg = read.next() => {
                let Some(msg) = msg else {
                    return Err(RoomError::Network("websocket stream ended".into()));
                };
                match msg.map_err(|e| RoomError::Network(e.to_string()))? {
                    Message::Binary(data) => {
                        let packets = protocol::parse_packets(&data);
                        for pkt in packets {
                            if pkt.op == protocol::OP_CONNECT_SUCCESS
                                && let Some(status) = status
                            {
                                status.mark_connected().await;
                            }
                            if let Err(e) = handle_packet(room_id, &pkt, producer, metrics).await {
                                match &e {
                                    RoomError::Protocol(_) => {
                                        tracing::warn!(error = %e, "handle_packet failed");
                                        metrics.parser_errors_total.inc();
                                    }
                                    _ => {
                                        tracing::warn!(error = %e, "handle_packet failed");
                                    }
                                }
                                return Err(e);
                            }
                        }
                    }
                    Message::Close(frame) => {
                        return Err(RoomError::Network(format!("websocket closed: {frame:?}")));
                    }
                    _ => {}
                }
            }
        }
    }
}

#[derive(Debug)]
enum RoomError {
    NoEndpoint,
    Endpoint(String),
    #[allow(dead_code)] // reserved for future auth rejection handling
    Auth(String),
    Network(String),
    Protocol(String),
    Publish(String),
}

impl std::fmt::Display for RoomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoomError::NoEndpoint => write!(f, "no endpoints"),
            RoomError::Endpoint(e) => write!(f, "endpoint: {e}"),
            RoomError::Auth(e) => write!(f, "auth: {e}"),
            RoomError::Network(e) => write!(f, "network: {e}"),
            RoomError::Protocol(e) => write!(f, "protocol: {e}"),
            RoomError::Publish(e) => write!(f, "publish: {e}"),
        }
    }
}

fn should_refresh_auth(error: &RoomError) -> bool {
    match error {
        RoomError::NoEndpoint | RoomError::Endpoint(_) | RoomError::Auth(_) => true,
        RoomError::Network(_) | RoomError::Protocol(_) | RoomError::Publish(_) => false,
    }
}

async fn handle_packet(
    room_id: u64,
    pkt: &ParsedPacket,
    producer: &RedpandaProducer,
    metrics: &bilive_sentinel::metrics::CollectorMetrics,
) -> std::result::Result<(), RoomError> {
    match pkt.op {
        protocol::OP_MESSAGE => {
            let inner_packets = match pkt.protover {
                protocol::PROTOVER_PLAIN => vec![pkt.clone()],
                protocol::PROTOVER_DEFLATE | protocol::PROTOVER_BROTLI => {
                    let decompressed = protocol::decompress_body(pkt.protover, &pkt.body)
                        .map_err(|e| RoomError::Protocol(format!("decompress: {e}")))?;
                    protocol::parse_packets(&decompressed)
                }
                _ => return Ok(()),
            };
            for inner in &inner_packets {
                if inner.op != protocol::OP_MESSAGE {
                    continue;
                }
                let messages = protocol::extract_json_messages(&inner.body);
                for msg in messages {
                    match protocol::parse_event(&msg) {
                        LiveEvent::Danmaku(ev) => {
                            publish_danmaku_with_retry(room_id, producer, metrics, &ev)
                                .await
                                .map_err(|e| RoomError::Publish(e.to_string()))?;
                            metrics.events_total.with_label_values(&["danmaku"]).inc();
                        }
                        LiveEvent::Gift(ev) => {
                            publish_gift_with_retry(room_id, producer, metrics, &ev)
                                .await
                                .map_err(|e| RoomError::Publish(e.to_string()))?;
                            metrics.events_total.with_label_values(&["gift"]).inc();
                        }
                        LiveEvent::Malformed { command } => {
                            tracing::warn!(command, "malformed event");
                            metrics.parser_errors_total.inc();
                        }
                        LiveEvent::Unsupported { .. } => {}
                    }
                }
            }
        }
        protocol::OP_HEARTBEAT_REPLY => {
            tracing::debug!("heartbeat reply");
        }
        protocol::OP_CONNECT_SUCCESS => {
            tracing::info!("connected");
        }
        _ => {}
    }
    Ok(())
}

async fn publish_danmaku_with_retry(
    room_id: u64,
    producer: &RedpandaProducer,
    metrics: &bilive_sentinel::metrics::CollectorMetrics,
    event: &protocol::DanmakuEvent,
) -> Result<()> {
    let mut last_error = None;
    for attempt in 1_u64..=3 {
        match producer.publish_danmaku(room_id, event).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                metrics
                    .publish_errors_total
                    .with_label_values(&["danmaku"])
                    .inc();
                tracing::warn!(room_id, attempt, error = %e, "publish danmaku failed");
                last_error = Some(e);
                tokio::time::sleep(Duration::from_millis(100 * attempt)).await;
            }
        }
    }
    Err(anyhow::anyhow!(
        "publish danmaku failed after retries: {}",
        last_error.unwrap_or_else(|| "unknown".into())
    ))
}

async fn publish_gift_with_retry(
    room_id: u64,
    producer: &RedpandaProducer,
    metrics: &bilive_sentinel::metrics::CollectorMetrics,
    event: &protocol::GiftEvent,
) -> Result<()> {
    let mut last_error = None;
    for attempt in 1_u64..=3 {
        match producer.publish_gift(room_id, event).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                metrics
                    .publish_errors_total
                    .with_label_values(&["gift"])
                    .inc();
                tracing::warn!(room_id, attempt, error = %e, "publish gift failed");
                last_error = Some(e);
                tokio::time::sleep(Duration::from_millis(100 * attempt)).await;
            }
        }
    }
    Err(anyhow::anyhow!(
        "publish gift failed after retries: {}",
        last_error.unwrap_or_else(|| "unknown".into())
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_auth_for_endpoint_errors() {
        assert!(should_refresh_auth(&RoomError::NoEndpoint));
        assert!(should_refresh_auth(&RoomError::Endpoint(
            "connection refused".into()
        )));
        assert!(should_refresh_auth(&RoomError::Endpoint(
            "websocket connect timed out".into()
        )));
    }

    #[test]
    fn refresh_auth_for_auth_errors() {
        assert!(should_refresh_auth(&RoomError::Auth(
            "auth rejected".into()
        )));
    }

    #[test]
    fn reuse_auth_for_network_errors() {
        assert!(!should_refresh_auth(&RoomError::Network(
            "websocket stream ended".into()
        )));
        assert!(!should_refresh_auth(&RoomError::Network(
            "connection reset".into()
        )));
    }

    #[test]
    fn reuse_auth_for_protocol_errors() {
        assert!(!should_refresh_auth(&RoomError::Protocol(
            "decompress: invalid data".into()
        )));
    }

    #[test]
    fn reuse_auth_for_publish_errors() {
        assert!(!should_refresh_auth(&RoomError::Publish(
            "publish failed after retries".into()
        )));
    }
}
