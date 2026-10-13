//! A process-wide debugging recorder. Tests run in parallel, so each test
//! reads only the series labelled with its own (unique) route name.
//!
//! `Snapshotter::snapshot` drains what it returns, so every snapshot is
//! folded into a running total that the assertions read.

use anyhow::Context;
use metrics_util::{
    CompositeKey,
    debugging::{DebugValue, DebuggingRecorder, Snapshotter},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, OnceLock, PoisonError},
};

static SNAPSHOTTER: Mutex<Option<Snapshotter>> = Mutex::new(None);

#[derive(Default)]
struct Totals {
    counters: HashMap<CompositeKey, u64>,
    gauges: HashSet<CompositeKey>,
}

static TOTALS: OnceLock<Mutex<Totals>> = OnceLock::new();

/// Install the recorder (once). Call before running a route whose metrics a
/// test asserts on.
pub fn install() -> anyhow::Result<()> {
    let mut slot = SNAPSHOTTER.lock().unwrap_or_else(PoisonError::into_inner);
    if slot.is_none() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder
            .install()
            .map_err(|e| anyhow::anyhow!("install debugging recorder: {e}"))?;
        *slot = Some(snapshotter);
    }
    Ok(())
}

fn with_totals<T>(f: impl FnOnce(&Totals) -> T) -> anyhow::Result<T> {
    let slot = SNAPSHOTTER.lock().unwrap_or_else(PoisonError::into_inner);
    let snapshotter = slot.as_ref().context("metrics::install() not called")?;
    let mut totals = TOTALS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        match value {
            DebugValue::Counter(n) => *totals.counters.entry(key).or_default() += n,
            DebugValue::Gauge(_) => {
                totals.gauges.insert(key);
            }
            DebugValue::Histogram(_) => {}
        }
    }
    Ok(f(&totals))
}

fn matches(key: &CompositeKey, name: &str, labels: &[(&str, &str)]) -> bool {
    key.key().name() == name
        && labels.iter().all(|(k, v)| {
            key.key()
                .labels()
                .any(|label| label.key() == *k && label.value() == *v)
        })
}

/// Total of counter `name` over series whose labels include all of `labels`.
pub fn counter(name: &str, labels: &[(&str, &str)]) -> anyhow::Result<u64> {
    with_totals(|totals| {
        totals
            .counters
            .iter()
            .filter(|(key, _)| matches(key, name, labels))
            .map(|(_, n)| n)
            .sum()
    })
}

/// Whether gauge `name` was set for a series matching `labels`.
pub fn gauge_exists(name: &str, labels: &[(&str, &str)]) -> anyhow::Result<bool> {
    with_totals(|totals| totals.gauges.iter().any(|key| matches(key, name, labels)))
}
