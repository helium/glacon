//! Metrics emitted through the `metrics` facade. The library never installs
//! an exporter; the app does (e.g. Prometheus).
//!
//! Every series carries a `route` label, plus `input` or `output` where it
//! applies. Times and durations are in milliseconds.

use chrono::{DateTime, Utc};
use std::time::Duration;

/// Batches an input's processor handed to the outputs.
pub const PROCESSED_BATCHES: &str = "glacon_processed_batches_total";
/// Milliseconds between the timestamp of the batch just processed and when
/// it was. Only moves when a batch is processed: to tell a stalled route,
/// use [`LAST_BATCH_TIMESTAMP`].
pub const BATCH_LAG: &str = "glacon_batch_lag_ms";
/// Timestamp (Unix milliseconds) of the newest batch processed. `now - this`
/// keeps growing while nothing is processed.
pub const LAST_BATCH_TIMESTAMP: &str = "glacon_last_batch_timestamp_ms";
/// Time a processor spent on one batch: the middleware, then handing it to
/// the outputs.
pub const BATCH_DURATION: &str = "glacon_batch_processing_ms";
/// Time the middleware took on one batch.
pub const MIDDLEWARE_DURATION: &str = "glacon_middleware_ms";
/// Time a processor waited for its outputs to take one batch: grows when
/// an output is slower than the input.
pub const OUTPUT_HANDOFF: &str = "glacon_output_handoff_ms";
/// Records the middleware turned into an `Err`, by `reason`
/// ([`GlaconRecordError::reason`](crate::GlaconRecordError::reason)).
pub const MIDDLEWARE_ERRORS: &str = "glacon_middleware_errors_total";
/// Results an output wrote, by `kind`: `ok` for records, `err` for the
/// middleware's errors (it may keep fewer of either).
pub const OUTPUT_RECORDS: &str = "glacon_output_records_total";
/// Batches an output wrote.
pub const OUTPUT_BATCHES: &str = "glacon_output_batches_total";
/// Time an output's `handle_data` took on one batch.
pub const OUTPUT_WRITE_DURATION: &str = "glacon_output_write_ms";
/// Batches read again because a previous run did not finish them.
pub const REPLAYED_BATCHES: &str = "glacon_replayed_batches_total";
/// Deliveries skipped because the output already had the batch, by
/// `reason`: `ledger` (it took it before the crash) or `already_written`
/// (the output's own hook said so).
pub const SKIPPED_DELIVERIES: &str = "glacon_skipped_deliveries_total";
/// Batches of an input read but not done (updated every minute).
pub const IN_FLIGHT_BATCHES: &str = "glacon_in_flight_batches";
/// Done batches removed from the ledger by retention.
pub const STATE_PRUNED: &str = "glacon_state_pruned_total";

/// `reason` of [`SKIPPED_DELIVERIES`]: the ledger had the delivery.
pub(crate) const SKIPPED_BY_LEDGER: &str = "ledger";
/// `reason` of [`SKIPPED_DELIVERIES`]: the output's `already_written`.
pub(crate) const SKIPPED_ALREADY_WRITTEN: &str = "already_written";

pub(crate) fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

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

pub(crate) fn middleware_errors(route: &str, input: &str, reason: &'static str, n: u64) {
    if n > 0 {
        metrics::counter!(
            MIDDLEWARE_ERRORS,
            "route" => route.to_string(),
            "input" => input.to_string(),
            "reason" => reason,
        )
        .increment(n);
    }
}

pub(crate) fn skipped_delivery(route: &str, output: &str, reason: &'static str) {
    metrics::counter!(
        SKIPPED_DELIVERIES,
        "route" => route.to_string(),
        "output" => output.to_string(),
        "reason" => reason,
    )
    .increment(1);
}

/// Times of one batch through a processor.
pub(crate) struct BatchTimes {
    /// The batch's own timestamp.
    pub timestamp: DateTime<Utc>,
    /// The newest batch timestamp this processor has seen, this one
    /// included.
    pub newest: DateTime<Utc>,
    pub middleware: Duration,
    pub handoff: Duration,
    pub total: Duration,
}

pub(crate) fn batch_processed(route: &str, input: &str, times: BatchTimes) {
    let labels = [("route", route.to_string()), ("input", input.to_string())];
    input_counter(PROCESSED_BATCHES, route, input, 1);
    let lag = (Utc::now() - times.timestamp).to_std().unwrap_or_default();
    metrics::gauge!(BATCH_LAG, &labels).set(ms(lag));
    metrics::gauge!(LAST_BATCH_TIMESTAMP, &labels).set(times.newest.timestamp_millis() as f64);
    metrics::histogram!(MIDDLEWARE_DURATION, &labels).record(ms(times.middleware));
    metrics::histogram!(OUTPUT_HANDOFF, &labels).record(ms(times.handoff));
    metrics::histogram!(BATCH_DURATION, &labels).record(ms(times.total));
}

/// One batch written by an output: `ok` records and `err` results.
pub(crate) fn output_wrote(route: &str, output: &str, ok: u64, err: u64, took: Duration) {
    let labels = [("route", route.to_string()), ("output", output.to_string())];
    output_counter(OUTPUT_BATCHES, route, output, 1);
    for (kind, n) in [("ok", ok), ("err", err)] {
        if n > 0 {
            metrics::counter!(
                OUTPUT_RECORDS,
                "route" => route.to_string(),
                "output" => output.to_string(),
                "kind" => kind,
            )
            .increment(n);
        }
    }
    metrics::histogram!(OUTPUT_WRITE_DURATION, &labels).record(ms(took));
}

pub(crate) fn state_pruned(route: &str, input: &str, n: u64) {
    input_counter(STATE_PRUNED, route, input, n);
}

pub(crate) fn in_flight(route: &str, input: &str, n: u64) {
    metrics::gauge!(IN_FLIGHT_BATCHES, "route" => route.to_string(), "input" => input.to_string())
        .set(n as f64);
}
