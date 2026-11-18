//! Metrics emitted through the `metrics` facade: glacon-core's, plus what
//! the file-store input and the Iceberg output add. Every series carries a
//! `route` label, plus `input` or `output` where it applies.

pub use glacon_core::metrics::*;

/// Messages decoded into records (`FileStoreInput`).
pub const RECORDS_DECODED: &str = "glacon_records_decoded_total";
/// Messages the input could not decode (`GlaconInput::decode` returned `None`).
pub const DECODE_FAILURES: &str = "glacon_decode_failures_total";
/// Rows queued to an output's table (`IcebergOutput`), by `kind`: `ok` for
/// rows made from records (`GlaconOutput::to_rows`), `err` for rows made from
/// the middleware's errors (`GlaconOutput::to_error_rows`).
pub const OUTPUT_ROWS: &str = "glacon_output_rows_total";

pub(crate) fn input_counter(name: &'static str, route: &str, input: &str, n: u64) {
    if n > 0 {
        metrics::counter!(name, "route" => route.to_string(), "input" => input.to_string())
            .increment(n);
    }
}

pub(crate) fn output_rows(route: &str, output: &str, kind: &'static str, n: u64) {
    if n > 0 {
        metrics::counter!(
            OUTPUT_ROWS,
            "route" => route.to_string(),
            "output" => output.to_string(),
            "kind" => kind,
        )
        .increment(n);
    }
}
