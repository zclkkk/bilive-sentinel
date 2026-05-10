use anyhow::Result;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use bilive_sentinel::{Config, init_tracing, new_service_registry, start_metrics_server};
use clap::Parser;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "config/default.toml")]
    config: String,
}

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[derive(Deserialize)]
struct AddRoomRequest {
    room_id: i64,
}

#[derive(Serialize)]
struct RoomResponse {
    room_id: i64,
    enabled: bool,
    last_connected_at: Option<String>,
    last_error: Option<String>,
}

#[derive(Serialize)]
struct LeaseResponse {
    room_id: i64,
    worker_id: String,
    leased_at: String,
    expires_at: String,
    last_heartbeat: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    init_tracing(&config.log.level);

    tracing::info!("api starting");

    let registry = new_service_registry();
    let metrics_addr = config.api.metrics_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(&metrics_addr, registry).await {
            tracing::error!(error = %e, "metrics server failed");
        }
    });

    let pool = PgPool::connect(&config.postgres.url)
        .await
        .map_err(|e| anyhow::anyhow!("postgres connect: {e}"))?;
    bilive_sentinel::registry::create_tables(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("create_tables: {e}"))?;

    let state = Arc::new(AppState { pool });

    let app = axum::Router::new()
        .route("/health", axum::routing::get(|| async { "ok" }))
        .route("/rooms", axum::routing::get(list_rooms).post(add_room))
        .route("/rooms/{room_id}/enable", axum::routing::put(enable_room))
        .route("/rooms/{room_id}/disable", axum::routing::put(disable_room))
        .route("/leases", axum::routing::get(list_leases))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&config.api.listen_addr).await?;
    tracing::info!(addr = %config.api.listen_addr, "api server started");
    axum::serve(listener, app).await?;

    Ok(())
}

async fn add_room(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddRoomRequest>,
) -> impl IntoResponse {
    match bilive_sentinel::registry::add_room(&state.pool, req.room_id).await {
        Ok(room) => (
            StatusCode::CREATED,
            Json(RoomResponse {
                room_id: room.room_id,
                enabled: room.enabled,
                last_connected_at: room.last_connected_at.map(|t| t.to_string()),
                last_error: room.last_error,
            }),
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn list_rooms(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match bilive_sentinel::registry::list_rooms(&state.pool).await {
        Ok(rooms) => {
            let resp: Vec<RoomResponse> = rooms
                .into_iter()
                .map(|r| RoomResponse {
                    room_id: r.room_id,
                    enabled: r.enabled,
                    last_connected_at: r.last_connected_at.map(|t| t.to_string()),
                    last_error: r.last_error,
                })
                .collect();
            Json(resp).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn enable_room(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<i64>,
) -> impl IntoResponse {
    match bilive_sentinel::registry::set_room_enabled(&state.pool, room_id, true).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn disable_room(
    State(state): State<Arc<AppState>>,
    Path(room_id): Path<i64>,
) -> impl IntoResponse {
    match bilive_sentinel::registry::set_room_enabled(&state.pool, room_id, false).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn list_leases(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match bilive_sentinel::registry::list_leases(&state.pool).await {
        Ok(leases) => {
            let resp: Vec<LeaseResponse> = leases
                .into_iter()
                .map(|l| LeaseResponse {
                    room_id: l.room_id,
                    worker_id: l.worker_id,
                    leased_at: l.leased_at.to_string(),
                    expires_at: l.expires_at.to_string(),
                    last_heartbeat: l.last_heartbeat.to_string(),
                })
                .collect();
            Json(resp).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
