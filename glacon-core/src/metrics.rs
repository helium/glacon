//! Metrics emitted through the `metrics` facade. The library never installs
//! an exporter; the app does (e.g. Prometheus).
//!
//! Every series carries a `route` label, plus `input` or `output` where it
//! applies.

use std::time::Duration;

/// Batches an input's processor handed to the outputs.
pub const PROCESSED_BATCHES: &str = "glacon_processed_batches_total";
/// Seconds between the newest processed batch's timestamp and now.
pub const BATCH_LAG: &str = "glacon_batch_lag_seconds";
/// Time a processor spent on one batch (middleware and hand-off).
pub const BATCH_DURATION: &str = "glacon_batch_processing_seconds";
/// Records the middleware sent nowhere.
pub const MIDDLEWARE_DROPPED: &str = "glacon_middleware_dropped_total";
/// Records routed to an output.
pub const OUTPUT_RECORDS: &str = "glacon_output_records_total";

pub(crate) fn input_counter(name: &'static str, route: &str, input: &str, n: u64) {
    if n > 0 {
        metrics::counter!(name, "route" => route.to_string(), "input" => input.to_string())
            .increment(n);
    }
}

pub(crate) fn output_counter(name: &'static str, route: &str, output: &str, n: u64) {
    if n > 0 {
        metrics::counter!(name, "route" => route.to_string(), "output" => output.to_string())
            .increment(n);
    }
}

pub(crate) fn batch_processed(route: &str, input: &str, lag: Duration, took: Duration) {
    input_counter(PROCESSED_BATCHES, route, input, 1);
    metrics::gauge!(BATCH_LAG, "route" => route.to_string(), "input" => input.to_string())
        .set(lag.as_secs_f64());
    metrics::histogram!(BATCH_DURATION, "route" => route.to_string(), "input" => input.to_string())
        .record(took);
}
