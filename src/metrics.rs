use prometheus::{Gauge, Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry};

#[derive(Clone)]
pub struct CollectorMetrics {
    pub active_rooms: Gauge,
    pub events_total: IntCounterVec,
    pub parser_errors_total: IntCounter,
}

#[derive(Clone)]
pub struct WriterMetrics {
    pub inserts_total: IntCounterVec,
    pub batch_size: Histogram,
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
        let parser_errors_total = IntCounter::with_opts(Opts::new(
            "bilive_parser_errors_total",
            "Total parser errors",
        ))
        .unwrap();

        registry.register(Box::new(active_rooms.clone())).unwrap();
        registry.register(Box::new(events_total.clone())).unwrap();
        registry
            .register(Box::new(parser_errors_total.clone()))
            .unwrap();

        Self {
            active_rooms,
            events_total,
            parser_errors_total,
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
        let batch_size = Histogram::with_opts(
            HistogramOpts::new("bilive_batch_size", "Insert batch size")
                .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0]),
        )
        .unwrap();

        registry.register(Box::new(inserts_total.clone())).unwrap();
        registry.register(Box::new(batch_size.clone())).unwrap();

        Self {
            inserts_total,
            batch_size,
        }
    }
}
