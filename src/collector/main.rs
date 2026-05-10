use anyhow::Result;
use bilive_sentinel::live_api::{LiveApi, LiveAuth};
use bilive_sentinel::protocol::{self, LiveEvent, OP_AUTH, OP_HEARTBEAT, ParsedPacket};
use bilive_sentinel::redpanda::RedpandaProducer;
use bilive_sentinel::{Config, init_tracing, new_service_registry, start_metrics_server};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
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
    let metrics_addr = config.collector.metrics_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(&metrics_addr, registry).await {
            tracing::error!(error = %e, "metrics server failed");
        }
    });

    bilive_sentinel::redpanda::ensure_topics(&config.redpanda.bootstrap_servers)
        .await
        .map_err(|e| anyhow::anyhow!("ensure_topics: {e}"))?;

    if let Some(room_id) = cli.room_id {
        run_single_room(&config, room_id).await
    } else {
        run_registry_mode(&config, cli.capacity).await
    }
}

async fn run_single_room(config: &Config, room_id: u64) -> Result<()> {
    let producer = RedpandaProducer::new(&config.redpanda.bootstrap_servers);
    let client = bilive_sentinel::live_api::LiveApiClient::new();

    tracing::info!(room_id, "fetching live auth");
    let auth = client
        .fetch_live_auth(room_id)
        .await
        .map_err(|e| anyhow::anyhow!("fetch_live_auth: {e}"))?;

    tracing::info!(room_id = auth.room_id, "connecting to room");
    run_room(&auth, &producer).await?;

    tracing::info!("collector shutting down");
    Ok(())
}

async fn run_registry_mode(config: &Config, capacity: usize) -> Result<()> {
    let pool = sqlx::PgPool::connect(&config.postgres.url)
        .await
        .map_err(|e| anyhow::anyhow!("postgres connect: {e}"))?;

    bilive_sentinel::registry::create_tables(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("create_tables: {e}"))?;

    let worker_id = uuid::Uuid::new_v4().to_string();
    let lease_ttl = chrono::Duration::seconds(60);

    tracing::info!(worker_id, capacity, "claiming rooms");
    let leases =
        bilive_sentinel::registry::claim_available_rooms(&pool, &worker_id, capacity, lease_ttl)
            .await
            .map_err(|e| anyhow::anyhow!("claim_available_rooms: {e}"))?;

    tracing::info!(count = leases.len(), "claimed rooms");

    let producer = RedpandaProducer::new(&config.redpanda.bootstrap_servers);
    let client = bilive_sentinel::live_api::LiveApiClient::new();

    let mut join_set = tokio::task::JoinSet::new();
    for lease in leases {
        let producer = producer.clone();
        let client = client.clone();
        let room_id = lease.room_id as u64;

        join_set.spawn(async move {
            match client.fetch_live_auth(room_id).await {
                Ok(auth) => {
                    if let Err(e) = run_room(&auth, &producer).await {
                        tracing::warn!(room_id, error = %e, "room failed");
                    }
                }
                Err(e) => {
                    tracing::warn!(room_id, error = %e, "fetch_live_auth failed");
                }
            }
        });
    }

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
                if let Err(e) = bilive_sentinel::registry::renew_lease(
                    &pool_clone,
                    lease.room_id,
                    &worker_id_clone,
                    chrono::Duration::seconds(60),
                )
                .await
                {
                    tracing::warn!(room_id = lease.room_id, error = %e, "renew lease failed");
                }
            }
        }
    });

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;

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

async fn check_live_auth(room_id: u64) -> Result<()> {
    let client = bilive_sentinel::live_api::LiveApiClient::new();
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

async fn run_room(auth: &LiveAuth, producer: &RedpandaProducer) -> Result<()> {
    let endpoint = auth
        .endpoints
        .first()
        .ok_or_else(|| anyhow::anyhow!("no endpoints"))?;
    let url = format!("wss://{}:{}/sub", endpoint.host, endpoint.port);

    tracing::info!(url, "connecting websocket");
    let (ws_stream, _) = connect_async(&url).await?;
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
    write.send(Message::Binary(auth_packet.into())).await?;
    tracing::info!("auth sent");

    // Spawn heartbeat task
    let mut write_hb = write;
    let heartbeat_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(20));
        loop {
            interval.tick().await;
            let packet = protocol::build_packet(OP_HEARTBEAT, "");
            if write_hb.send(Message::Binary(packet.into())).await.is_err() {
                break;
            }
        }
    });

    // Read loop
    let room_id = auth.room_id;
    while let Some(msg) = read.next().await {
        let msg = msg?;
        match msg {
            Message::Binary(data) => {
                let packets = protocol::parse_packets(&data);
                for pkt in packets {
                    if let Err(e) = handle_packet(room_id, &pkt, producer).await {
                        tracing::warn!(error = %e, "handle_packet failed");
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    heartbeat_handle.abort();
    Ok(())
}

async fn handle_packet(
    room_id: u64,
    pkt: &ParsedPacket,
    producer: &RedpandaProducer,
) -> Result<()> {
    match pkt.op {
        protocol::OP_MESSAGE => {
            let body = match pkt.protover {
                protocol::PROTOVER_PLAIN => pkt.body.clone(),
                protocol::PROTOVER_DEFLATE | protocol::PROTOVER_BROTLI => {
                    protocol::decompress_body(pkt.protover, &pkt.body)
                        .map_err(|e| anyhow::anyhow!("decompress: {e}"))?
                }
                _ => return Ok(()),
            };
            let messages = protocol::extract_json_messages(&body);
            for msg in messages {
                match protocol::parse_event(&msg) {
                    LiveEvent::Danmaku(ev) => {
                        if let Err(e) = producer.publish_danmaku(room_id, &ev).await {
                            tracing::warn!(error = %e, "publish danmaku failed");
                        }
                    }
                    LiveEvent::Gift(ev) => {
                        if let Err(e) = producer.publish_gift(room_id, &ev).await {
                            tracing::warn!(error = %e, "publish gift failed");
                        }
                    }
                    LiveEvent::Unsupported { .. } => {}
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
