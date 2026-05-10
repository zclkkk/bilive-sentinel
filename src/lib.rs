use anyhow::Result;
use prometheus::{Encoder, Gauge, Registry, TextEncoder};
use serde::Deserialize;

pub mod clickhouse;
pub mod live_api;
pub mod protocol;
pub mod redpanda;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub log: LogConfig,
    pub collector: CollectorConfig,
    pub writer: WriterConfig,
    pub api: ApiConfig,
    pub postgres: PostgresConfig,
    pub clickhouse: ClickHouseConfig,
    pub redpanda: RedpandaConfig,
}

#[derive(Debug, Deserialize)]
pub struct LogConfig {
    pub level: String,
}

#[derive(Debug, Deserialize)]
pub struct CollectorConfig {
    pub metrics_addr: String,
}

#[derive(Debug, Deserialize)]
pub struct WriterConfig {
    pub metrics_addr: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiConfig {
    pub listen_addr: String,
    pub metrics_addr: String,
}

#[derive(Debug, Deserialize)]
pub struct PostgresConfig {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct ClickHouseConfig {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct RedpandaConfig {
    pub bootstrap_servers: String,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}

pub fn init_tracing(level: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level)),
        )
        .init();
}

pub fn new_service_registry() -> Registry {
    let registry = Registry::new();
    let up = Gauge::new("bilive_sentinel_up", "Whether the service is running")
        .expect("metric name is valid");
    up.set(1.0);
    registry
        .register(Box::new(up))
        .expect("metric registration");
    registry
}

pub async fn start_metrics_server(addr: &str, registry: Registry) -> Result<()> {
    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || {
            let registry = registry.clone();
            async move {
                let encoder = TextEncoder::new();
                let metric_families = registry.gather();
                let mut buffer = Vec::new();
                encoder
                    .encode(&metric_families, &mut buffer)
                    .unwrap_or_default();
                String::from_utf8(buffer).unwrap_or_default()
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(addr, "metrics server started");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parse() {
        let toml_str = r#"
[log]
level = "info"

[collector]
metrics_addr = "0.0.0.0:9100"

[writer]
metrics_addr = "0.0.0.0:9101"

[api]
listen_addr = "0.0.0.0:8080"
metrics_addr = "0.0.0.0:9102"

[postgres]
url = "postgres://bilive:bilive@localhost:5432/bilive"

[clickhouse]
url = "http://localhost:8123"

[redpanda]
bootstrap_servers = "localhost:9092"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.log.level, "info");
        assert_eq!(config.collector.metrics_addr, "0.0.0.0:9100");
        assert_eq!(config.api.listen_addr, "0.0.0.0:8080");
    }
}
