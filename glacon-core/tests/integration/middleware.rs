//! The middleware: records from N inputs, transformed by each input's
//! middleware (changing their type, dropping some), then kept or not by each
//! of M outputs; a failing middleware fails the route.

use crate::common::{self, End, Journal, MemoryInput, MemoryOutput, batch, event};
use glacon_core::{
    BatchId, BoxError, Error, GlaconCoreMiddleware, GlaconCoreOutput, InitializedOutput,
    OutputContext, Route, async_trait,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

// ---------------------------------------------------------------- transforming

#[tokio::test]
async fn routes_records_to_outputs() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = (
        MemoryOutput::new(common::LEFT, &journal),
        MemoryOutput::new(common::RIGHT, &journal),
    );
    let us = MemoryInput::new(
        "us",
        vec![
            batch(
                "us.1",
                vec![
                    event("l1", "left"),
                    event("b1", "both"),
                    event("d1", "drop"),
                ],
            ),
            batch("us.2", vec![event("r1", "right")]),
        ],
        End::Return,
        &journal,
    );
    let eu = MemoryInput::new(
        "eu",
        vec![batch(
            "eu.1",
            vec![event("l2", "left"), event("b2", "both")],
        )],
        End::Return,
        &journal,
    );

    let route = common::route(
        "routing",
        state.path(),
        vec![us, eu],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route).await?;

    assert_eq!(left.rows(), ["b1", "b2", "l1", "l2"]);
    assert_eq!(right.rows(), ["b1", "b2", "r1"]);
    Ok(())
}

/// A speed test once verified: the middleware decides, every output sees
/// the result and keeps what it wants.
#[derive(Debug, Clone)]
struct Verified {
    id: String,
    invalid_reason: Option<&'static str>,
}

/// `common::Event` → [`Verified`]: ids starting with "slow" are invalid,
/// "drop" events are malformed and dropped.
#[derive(Clone)]
struct Verify;

#[async_trait]
impl GlaconCoreMiddleware<common::Event, Verified> for Verify {
    async fn handle_data(
        &mut self,
        _: &BatchId,
        events: Vec<common::Event>,
    ) -> Result<Vec<Verified>, BoxError> {
        Ok(events
            .into_iter()
            .filter(|e| e.kind != "drop")
            .map(|e| Verified {
                invalid_reason: e.id.starts_with("slow").then_some("too slow"),
                id: e.id,
            })
            .collect())
    }
}

/// Keeps the valid [`Verified`] records, or the invalid ones with their
/// reason.
#[derive(Clone)]
struct VerifiedOutput {
    invalid: bool,
    rows: Arc<Mutex<Vec<String>>>,
}

impl VerifiedOutput {
    fn new(invalid: bool) -> Self {
        Self {
            invalid,
            rows: Default::default(),
        }
    }

    fn rows(&self) -> Vec<String> {
        let mut rows = self
            .rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        rows.sort();
        rows
    }
}

#[async_trait]
impl GlaconCoreOutput<Verified> for VerifiedOutput {
    type State = ();

    async fn init(&mut self, _: &OutputContext) -> Result<InitializedOutput<()>, BoxError> {
        Ok(InitializedOutput::new(()))
    }

    async fn handle_data(
        &mut self,
        _: &mut (),
        _: &BatchId,
        records: &[Verified],
    ) -> Result<(), BoxError> {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        for record in records {
            match (self.invalid, record.invalid_reason) {
                (false, None) => rows.push(record.id.clone()),
                (true, Some(reason)) => rows.push(format!("{}: {reason}", record.id)),
                _ => {}
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_middleware_can_change_the_record_type() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let input = MemoryInput::new(
        "us",
        vec![batch(
            "us.1",
            vec![
                event("fast1", "both"),
                event("slow1", "both"),
                event("broken", "drop"),
                event("fast2", "both"),
            ],
        )],
        End::Return,
        &journal,
    );
    let (verified, invalid) = (VerifiedOutput::new(false), VerifiedOutput::new(true));

    let route = Route::builder("verify", common::settings(state.path(), "verify"))
        .input("us", input)
        .middleware(Verify)
        .output("verified", verified.clone())
        .output("invalid", invalid.clone())
        .build()?;
    common::run(route).await?;

    assert_eq!(verified.rows(), ["fast1", "fast2"]);
    assert_eq!(invalid.rows(), ["slow1: too slow"]);
    Ok(())
}

/// Deduplication is a middleware dropping duplicates: every processor gets
/// its own instance, sharing what was seen through an Arc.
///
/// Replay-safe: a batch read again after a crash must give the same
/// records, so each id remembers the batch that claimed it, and a record
/// whose id this same batch claimed is kept. (A real one would keep this in
/// a store, like the ledger.)
#[derive(Clone, Default)]
struct Dedup {
    claimed_by: Arc<Mutex<HashMap<String, BatchId>>>,
}

#[async_trait]
impl GlaconCoreMiddleware<common::Event, common::Event> for Dedup {
    async fn handle_data(
        &mut self,
        batch: &BatchId,
        events: Vec<common::Event>,
    ) -> Result<Vec<common::Event>, BoxError> {
        let mut claimed_by = self
            .claimed_by
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut in_batch = HashSet::new();
        Ok(events
            .into_iter()
            .filter(|e| {
                let claimer = claimed_by
                    .entry(e.id.clone())
                    .or_insert_with(|| batch.clone());
                claimer == batch && in_batch.insert(e.id.clone())
            })
            .collect())
    }
}

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
    Ok(())
}

// ---------------------------------------------------------------- dropping and failing

#[tokio::test]
async fn a_batch_the_middleware_empties_is_done() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let input = MemoryInput::new(
        "us",
        vec![batch("us.1", vec![event("x", "drop")])],
        End::Return,
        &journal,
    );

    let route = common::route(
        "dropped",
        state.path(),
        vec![input],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route).await?;

    assert!(left.batches().is_empty() && right.batches().is_empty());
    let ledger = common::ledger(state.path(), "dropped").await?;
    assert!(ledger.is_done("us", "us.1").await?);
    common::assert_nothing_in_flight(state.path(), "dropped").await
}

#[tokio::test]
async fn a_failing_middleware_fails_the_route() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let input = MemoryInput::new(
        "us",
        vec![batch("us.1", vec![event("x", "fail")])],
        End::HoldOpen,
        &journal,
    );

    let route = common::route(
        "middleware",
        state.path(),
        vec![input],
        &left,
        &right,
        &journal,
    )
    .build()?;
    let err = common::run(route).await.expect_err("must fail");

    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Middleware { input, .. } if input == "us"),
        "{err}"
    );
    common::assert_down_order(&journal);
    Ok(())
}
