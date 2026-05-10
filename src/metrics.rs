use prometheus::{
    Gauge, GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry,
};

#[derive(Clone)]
pub struct CollectorMetrics {
    pub active_rooms: Gauge,
    pub events_total: IntCounterVec,
    pub publish_errors_total: IntCounterVec,
    pub parser_errors_total: IntCounter,
    pub reconnects_total: IntCounter,
}

#[derive(Clone)]
pub struct WriterMetrics {
    pub inserts_total: IntCounterVec,
    pub commit_errors_total: IntCounterVec,
    pub batch_size: Histogram,
    pub insert_latency: Histogram,
    pub consumer_lag: GaugeVec,
    pub bad_messages_total: IntCounterVec,
}

impl CollectorMetrics {
    pub fn register(registry: &Registry) -> Self {
        let active_rooms =
            Gauge::with_opts(Opts::new("bilive_active_rooms", "Number of active rooms")).unwrap();
        let events_total = IntCounterVec::new(
            Opts::new("bilive_events_total", "Total events processed"),
            &["type"],
        )
        .unwrap();
        let publish_errors_total = IntCounterVec::new(
            Opts::new(
                "bilive_publish_errors_total",
                "Total event publish failures",
            ),
            &["type"],
        )
        .unwrap();
        let parser_errors_total = IntCounter::with_opts(Opts::new(
            "bilive_parser_errors_total",
            "Total parser errors",
        ))
        .unwrap();
        let reconnects_total = IntCounter::with_opts(Opts::new(
            "bilive_reconnects_total",
            "Total reconnection attempts",
        ))
        .unwrap();

        registry.register(Box::new(active_rooms.clone())).unwrap();
        registry.register(Box::new(events_total.clone())).unwrap();
        registry
            .register(Box::new(publish_errors_total.clone()))
            .unwrap();
        registry
            .register(Box::new(parser_errors_total.clone()))
            .unwrap();
        registry
            .register(Box::new(reconnects_total.clone()))
            .unwrap();

        Self {
            active_rooms,
            events_total,
            publish_errors_total,
            parser_errors_total,
            reconnects_total,
        }
    }
}

impl WriterMetrics {
    pub fn register(registry: &Registry) -> Self {
        let inserts_total = IntCounterVec::new(
            Opts::new("bilive_inserts_total", "Total ClickHouse inserts"),
            &["table"],
        )
        .unwrap();
        let commit_errors_total = IntCounterVec::new(
            Opts::new("bilive_commit_errors_total", "Total Redpanda commit errors"),
            &["table"],
        )
        .unwrap();
        let batch_size = Histogram::with_opts(
            HistogramOpts::new("bilive_batch_size", "Insert batch size")
                .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0]),
        )
        .unwrap();
        let insert_latency = Histogram::with_opts(
            HistogramOpts::new(
                "bilive_insert_latency_seconds",
                "ClickHouse insert duration",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
            ]),
        )
        .unwrap();
        let consumer_lag = GaugeVec::new(
            Opts::new(
                "bilive_consumer_lag",
                "Redpanda consumer lag (position minus committed offset)",
            ),
            &["topic"],
        )
        .unwrap();

        let bad_messages_total = IntCounterVec::new(
            Opts::new(
                "bilive_bad_messages_total",
                "Total messages that could not be deserialized",
            ),
            &["topic"],
        )
        .unwrap();

        registry.register(Box::new(inserts_total.clone())).unwrap();
        registry
            .register(Box::new(commit_errors_total.clone()))
            .unwrap();
        registry.register(Box::new(batch_size.clone())).unwrap();
        registry.register(Box::new(insert_latency.clone())).unwrap();
        registry.register(Box::new(consumer_lag.clone())).unwrap();
        registry
            .register(Box::new(bad_messages_total.clone()))
            .unwrap();

        Self {
            inserts_total,
            commit_errors_total,
            batch_size,
            insert_latency,
            consumer_lag,
            bad_messages_total,
        }
    }
}
