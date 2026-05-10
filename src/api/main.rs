use anyhow::Result;
use bilive_sentinel::{Config, init_tracing, new_service_registry, start_metrics_server};
use clap::Parser;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "config/default.toml")]
    config: String,
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

    let app = axum::Router::new().route("/health", axum::routing::get(|| async { "ok" }));

    let listener = tokio::net::TcpListener::bind(&config.api.listen_addr).await?;
    tracing::info!(addr = %config.api.listen_addr, "api server started");
    axum::serve(listener, app).await?;

    Ok(())
}
