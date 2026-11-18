//! Deduplication as a middleware: a record seen in one batch is a duplicate
//! in every other batch, across inputs, within a batch, and across a crash,
//! where a replayed batch keeps the records it claimed.

use crate::common::{self, End, Journal, MemoryInput, MemoryOutput, batch, event};
use glacon_core::{
    BatchId, BoxError, Error, GlaconCoreMiddleware, GlaconRecordError, RecordResult, Route,
    async_trait,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

// ---------------------------------------------------------------- tests

#[tokio::test]
async fn deduplication_is_a_middleware() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let output = MemoryOutput::new(common::LEFT, &journal);
    let inputs = [("us", ["a", "b", "a"]), ("eu", ["b", "c", "c"])].map(|(name, ids)| {
        let events = ids.iter().map(|id| event(id, "both")).collect();
        MemoryInput::new(name, vec![batch(name, events)], End::Return, &journal)
    });

    let mut builder = Route::builder("dedup", common::settings(state.path(), "dedup"));
    for input in inputs {
        builder = builder.input(input.name, input);
    }
    let route = builder
        .middleware(Dedup::default())
        .output(common::LEFT, output.clone())
        .build()?;
    common::run(route).await?;

    assert_eq!(output.rows(), ["a", "b", "c"]);
    assert_eq!(
        output.errors(),
        ["a: duplicate", "b: duplicate", "c: duplicate"]
    );
    Ok(())
}

#[tokio::test]
async fn deduplication_keeps_a_replayed_batch() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let output = MemoryOutput::new(common::LEFT, &journal);
    // Its claims outlive a run, like a store on disk.
    let dedup = Dedup::default();
    let file = |key, ids: &[&str]| batch(key, ids.iter().map(|id| event(id, "both")).collect());
    let route = |batches| {
        Route::builder("replay", common::settings(state.path(), "replay"))
            .input("us", MemoryInput::new("us", batches, End::Return, &journal))
            .middleware(dedup.clone())
            .output(common::LEFT, output.clone())
            .build()
    };

    // The middleware claims a and b for us.1, then the output fails: a crash
    // before anything is written.
    output.fail_writes(1);
    let err = common::run(route(vec![file("us.1", &["a", "b", "a"])])?)
        .await
        .expect_err("the output fails");
    assert!(matches!(common::route_error(&err), Error::Output { .. }));
    assert!(output.batches().is_empty());

    // us.1 is replayed and keeps what it claimed, not calling its own
    // records duplicates; us.2 is new, and only c is its own.
    common::run(route(vec![
        file("us.1", &["a", "b", "a"]),
        file("us.2", &["a", "c"]),
    ])?)
    .await?;

    assert_eq!(output.rows(), ["a", "b", "c"]);
    assert_eq!(output.errors(), ["a: duplicate", "a: duplicate"]);
    assert_eq!(output.batches(), ["us.1", "us.2"]);
    common::assert_nothing_in_flight(state.path(), "replay").await
}

// ---------------------------------------------------------------- helpers

/// Deduplication is a middleware rejecting duplicates: every processor gets
/// its own instance, sharing what was seen through an Arc.
///
/// Replay-safe: a batch read again after a crash must give the same
/// results, so each id remembers the batch that claimed it, and a record
/// whose id this same batch claimed is still `Ok`. (A real one would keep
/// this in a store, like the ledger.) A batch is its input and key, as in
/// the ledger: its timestamp may differ when it is produced again.
#[derive(Clone, Default)]
struct Dedup {
    claimed_by: Arc<Mutex<HashMap<String, Claimer>>>,
}

/// A record whose id another batch, or this one, already claimed.
#[derive(Debug, thiserror::Error)]
#[error("{id}: duplicate")]
struct Duplicate {
    id: String,
}

impl GlaconRecordError for Duplicate {
    fn reason(&self) -> &'static str {
        "duplicate"
    }
}

/// The batch that claimed a record: its input and key.
type Claimer = (Arc<str>, String);

#[async_trait]
impl GlaconCoreMiddleware<common::Event, common::Event> for Dedup {
    type Error = Duplicate;

    async fn handle_data(
        &mut self,
        batch: &BatchId,
        events: Vec<common::Event>,
    ) -> Result<Vec<RecordResult<common::Event, Duplicate>>, BoxError> {
        let mut claimed_by = self
            .claimed_by
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let this = (batch.input.clone(), batch.key.clone());
        let mut in_batch = HashSet::new();
        Ok(events
            .into_iter()
            .map(|e| {
                let claimer = claimed_by
                    .entry(e.id.clone())
                    .or_insert_with(|| this.clone());
                if *claimer == this && in_batch.insert(e.id.clone()) {
                    Ok(e)
                } else {
                    Err(Duplicate { id: e.id })
                }
            })
            .collect())
    }
}
