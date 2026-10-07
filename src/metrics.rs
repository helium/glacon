//! Metrics emitted through the `metrics` facade. The library never installs
//! an exporter; the app does (e.g. Prometheus).
//!
//! Every series carries a `route` label, plus `input` or `output` where it
//! applies.

use std::time::Duration;

/// Files fully processed.
pub const PROCESSED_FILES: &str = "glacon_processed_files_total";
/// Seconds between the newest processed file's timestamp and now.
pub const FILE_LAG: &str = "glacon_file_lag_seconds";
/// Time spent processing one file (decode to state commit).
pub const FILE_DURATION: &str = "glacon_file_processing_seconds";
/// Messages decoded into records.
pub const RECORDS_DECODED: &str = "glacon_records_decoded_total";
/// Messages the input could not decode (`GlaconInput::decode` returned `None`).
pub const DECODE_FAILURES: &str = "glacon_decode_failures_total";
/// Records dropped as duplicates.
pub const DEDUP_DROPPED: &str = "glacon_dedup_dropped_total";
/// Records the middleware sent nowhere.
pub const MIDDLEWARE_DROPPED: &str = "glacon_middleware_dropped_total";
/// Records routed to an output.
pub const OUTPUT_RECORDS: &str = "glacon_output_records_total";
/// Rows queued to an output's table.
pub const OUTPUT_ROWS: &str = "glacon_output_rows_total";
/// Rows removed from the state db by retention (`kind` = processed_files | dedup_keys).
pub const STATE_PRUNED: &str = "glacon_state_pruned_total";

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

pub(crate) fn route_counter(name: &'static str, route: &str, n: u64) {
    if n > 0 {
        metrics::counter!(name, "route" => route.to_string()).increment(n);
    }
}

pub(crate) fn file_processed(route: &str, input: &str, lag: Duration, took: Duration) {
    input_counter(PROCESSED_FILES, route, input, 1);
    metrics::gauge!(FILE_LAG, "route" => route.to_string(), "input" => input.to_string())
        .set(lag.as_secs_f64());
    metrics::histogram!(FILE_DURATION, "route" => route.to_string(), "input" => input.to_string())
        .record(took);
}

pub(crate) fn state_pruned(route: &str, kind: &'static str, n: u64) {
    if n > 0 {
        metrics::counter!(STATE_PRUNED, "route" => route.to_string(), "kind" => kind).increment(n);
    }
}
