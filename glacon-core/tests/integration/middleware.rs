//! The middleware: records from N inputs, transformed by each input's
//! middleware (changing their type, rejecting some), then kept or not by
//! each of M outputs, which also get the rejections; a failing middleware,
//! or one not returning a result per record, fails the route.

use crate::common::{self, End, Journal, MemoryInput, MemoryOutput, batch, event};
use glacon_core::{
    BatchId, BoxError, Error, GlaconCoreMiddleware, GlaconCoreOutput, InitializedOutput,
    OutputContext, RecordResult, Route, async_trait,
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
                    event("d1", "reject"),
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
    // Every output gets the rejections too.
    assert_eq!(left.errors(), ["d1: rejected"]);
    assert_eq!(right.errors(), ["d1: rejected"]);
    Ok(())
}

/// A speed test once verified: the middleware decides, every output sees
/// the result and keeps what it wants.
#[derive(Debug, Clone)]
struct Verified {
    id: String,
}

/// `common::Event` → [`Verified`]: ids starting with "slow" are invalid,
/// "reject" events are malformed.
#[derive(Clone)]
struct Verify;

#[async_trait]
impl GlaconCoreMiddleware<common::Event, Verified> for Verify {
    async fn handle_data(
        &mut self,
        _: &BatchId,
        events: Vec<common::Event>,
    ) -> Result<Vec<RecordResult<Verified>>, BoxError> {
        Ok(events
            .into_iter()
            .map(|e| match e.kind {
                "reject" => Err(format!("{}: malformed", e.id)),
                _ if e.id.starts_with("slow") => Err(format!("{}: too slow", e.id)),
                _ => Ok(Verified { id: e.id }),
            })
            .collect())
    }
}

/// Keeps the valid [`Verified`] records, or the errors.
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
        records: &[RecordResult<Verified>],
    ) -> Result<(), BoxError> {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        for record in records {
            match (self.invalid, record) {
                (false, Ok(verified)) => rows.push(verified.id.clone()),
                (true, Err(reason)) => rows.push(reason.clone()),
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
                event("broken", "reject"),
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
    assert_eq!(invalid.rows(), ["broken: malformed", "slow1: too slow"]);
    Ok(())
}

/// Deduplication is a middleware rejecting duplicates: every processor gets
/// its own instance, sharing what was seen through an Arc.
///
/// Replay-safe: a batch read again after a crash must give the same
/// results, so each id remembers the batch that claimed it, and a record
/// whose id this same batch claimed is still `Ok`. (A real one would keep this in
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
    ) -> Result<Vec<RecordResult<common::Event>>, BoxError> {
        let mut claimed_by = self
            .claimed_by
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut in_batch = HashSet::new();
        Ok(events
            .into_iter()
            .map(|e| {
                let claimer = claimed_by
                    .entry(e.id.clone())
                    .or_insert_with(|| batch.clone());
                if claimer == batch && in_batch.insert(e.id.clone()) {
                    Ok(e)
                } else {
                    Err(format!("{}: duplicate", e.id))
                }
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
    assert_eq!(
        output.errors(),
        ["a: duplicate", "b: duplicate", "c: duplicate"]
    );
    Ok(())
}

// ---------------------------------------------------------------- rejecting and failing

#[tokio::test]
async fn a_batch_of_only_rejections_still_reaches_the_outputs() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let input = MemoryInput::new(
        "us",
        vec![batch("us.1", vec![event("x", "reject")])],
        End::Return,
        &journal,
    );

    let route = common::route(
        "rejected",
        state.path(),
        vec![input],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route).await?;

    for output in [&left, &right] {
        assert_eq!(output.batches(), ["us.1"]);
        assert!(output.rows().is_empty());
        assert_eq!(output.errors(), ["x: rejected"]);
    }
    let ledger = common::ledger(state.path(), "rejected").await?;
    assert!(ledger.is_done("us", "us.1").await?);
    common::assert_nothing_in_flight(state.path(), "rejected").await
}

/// Leaves out every "reject" event instead of returning an error for it.
#[derive(Clone)]
struct Dropping;

#[async_trait]
impl GlaconCoreMiddleware<common::Event, common::Event> for Dropping {
    async fn handle_data(
        &mut self,
        _: &BatchId,
        events: Vec<common::Event>,
    ) -> Result<Vec<RecordResult<common::Event>>, BoxError> {
        Ok(events
            .into_iter()
            .filter(|e| e.kind != "reject")
            .map(Ok)
            .collect())
    }
}

#[tokio::test]
async fn a_middleware_dropping_records_fails_the_route() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let output = MemoryOutput::new(common::LEFT, &journal);
    let input = MemoryInput::new(
        "us",
        vec![batch(
            "us.1",
            vec![event("a", "both"), event("x", "reject")],
        )],
        End::HoldOpen,
        &journal,
    );

    let route = Route::builder("dropping", common::settings(state.path(), "dropping"))
        .input("us", input)
        .middleware(Dropping)
        .output(common::LEFT, output.clone())
        .build()?;
    let err = common::run(route).await.expect_err("must fail");

    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Middleware { input, .. } if input == "us"),
        "{err}"
    );
    assert!(
        err.to_string()
            .contains("returned 1 results for the 2 records"),
        "{err}"
    );
    assert!(output.batches().is_empty());
    Ok(())
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
