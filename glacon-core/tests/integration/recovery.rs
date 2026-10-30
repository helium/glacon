//! Getting back on its feet: a route run again on the same state dir after
//! a failure delivers every batch once to every output.

use crate::common::{self, End, Journal, MemoryInput, MemoryOutput, batch, event};
use glacon_core::{Error, FreshState};
use std::path::Path;

fn outputs(journal: &Journal) -> (MemoryOutput, MemoryOutput) {
    (
        MemoryOutput::new(common::LEFT, journal),
        MemoryOutput::new(common::RIGHT, journal),
    )
}

/// One input, `us`, producing batches `us.<n>` of one event each, kept by
/// both outputs.
fn input(batches: impl IntoIterator<Item = u32>, end: End, journal: &Journal) -> MemoryInput {
    let batches = batches
        .into_iter()
        .map(|n| batch(&format!("us.{n}"), vec![event(&format!("e{n}"), "both")]))
        .collect();
    MemoryInput::new("us", batches, end, journal)
}

async fn assert_nothing_in_flight(state: &Path, route: &str) -> anyhow::Result<()> {
    let ledger = common::ledger(state, route).await?;
    assert_eq!(ledger.in_flight("us").await?, 0);
    Ok(())
}

#[tokio::test]
async fn a_failed_output_gets_the_batch_again_alone() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    right.fail_writes(1);

    let route = common::route(
        "retry",
        state.path(),
        vec![input([1], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    let err = common::run(route).await.expect_err("right fails");
    assert!(matches!(common::route_error(&err), Error::Output { .. }));
    assert!(right.batches().is_empty());

    let route = common::route(
        "retry",
        state.path(),
        vec![input([1], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route).await?;

    assert_eq!(left.batches(), ["us.1"], "left written once");
    assert_eq!(right.batches(), ["us.1"]);
    assert_nothing_in_flight(state.path(), "retry").await
}

#[tokio::test]
async fn a_crash_after_a_write_duplicates_it_without_already_written() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    right.fail_after_writes(1);

    for _ in 0..2 {
        let route = common::route(
            "duplicate",
            state.path(),
            vec![input([1], End::Return, &journal)],
            &left,
            &right,
            &journal,
        )
        .build()?;
        let _ = common::run(route).await;
    }

    assert_eq!(left.batches(), ["us.1"]);
    assert_eq!(right.batches(), ["us.1", "us.1"], "the accepted window");
    assert_nothing_in_flight(state.path(), "duplicate").await
}

#[tokio::test]
async fn already_written_closes_the_duplicate_window() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, mut right) = outputs(&journal);
    right.knows_written = true;
    right.fail_after_writes(1);

    let route = common::route(
        "written",
        state.path(),
        vec![input([1], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route)
        .await
        .expect_err("right fails after writing");

    let route = common::route(
        "written",
        state.path(),
        vec![input([1], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route).await?;

    assert_eq!(left.batches(), ["us.1"]);
    assert_eq!(right.batches(), ["us.1"]);
    assert_nothing_in_flight(state.path(), "written").await
}

#[tokio::test]
async fn done_batches_are_skipped() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = outputs(&journal);

    for batches in [1..=2, 1..=3] {
        let route = common::route(
            "skip",
            state.path(),
            vec![input(batches, End::Return, &journal)],
            &left,
            &right,
            &journal,
        )
        .build()?;
        common::run(route).await?;
    }

    assert_eq!(left.batches(), ["us.1", "us.2", "us.3"]);
    assert_eq!(right.batches(), ["us.1", "us.2", "us.3"]);
    Ok(())
}

#[tokio::test]
async fn a_batch_the_middleware_empties_is_done() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
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
    assert_nothing_in_flight(state.path(), "dropped").await
}

#[tokio::test]
async fn an_output_that_keeps_nothing_still_acknowledges() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    let input = MemoryInput::new(
        "us",
        vec![batch("us.1", vec![event("r", "right")])],
        End::Return,
        &journal,
    );

    let route = common::route(
        "nothing",
        state.path(),
        vec![input],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route).await?;

    // Both outputs were handed the batch; only right kept something.
    assert_eq!(left.batches(), ["us.1"]);
    assert!(left.rows().is_empty());
    assert_eq!(right.rows(), ["r"]);
    let ledger = common::ledger(state.path(), "nothing").await?;
    assert!(ledger.is_done("us", "us.1").await?);
    assert_nothing_in_flight(state.path(), "nothing").await
}

#[tokio::test]
async fn a_restart_after_shutdown_loses_and_duplicates_nothing() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = outputs(&journal);

    let route = common::route(
        "restart",
        state.path(),
        vec![input(1..=5, End::HoldOpen, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run_until(route, left.wait_for_rows(2)).await?;

    // The input produces everything again; the route skips what is done.
    let route = common::route(
        "restart",
        state.path(),
        vec![input(1..=5, End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route).await?;

    let all = ["us.1", "us.2", "us.3", "us.4", "us.5"];
    assert_eq!(left.batches(), all);
    assert_eq!(right.batches(), all);
    assert_nothing_in_flight(state.path(), "restart").await
}

#[tokio::test]
async fn an_empty_ledger_refuses_outputs_with_data() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    // Data written by a route with another ledger (say, a lost volume).
    let seed = common::route(
        "seed",
        state.path(),
        vec![input([1], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(seed).await?;

    let refused = common::route(
        "fresh",
        state.path(),
        vec![input([1], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    let err = common::run(refused).await.expect_err("must refuse");
    assert!(
        matches!(common::route_error(&err), Error::FreshStateRefused { output, .. } if output == common::LEFT),
        "{err}"
    );
    assert_eq!(journal.positions("terminate output left").len(), 2);
    assert_eq!(left.batches(), ["us.1"], "nothing written");

    let proceed = common::route(
        "fresh",
        state.path(),
        vec![input([1], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .fresh_state(FreshState::Proceed)
    .build()?;
    common::run(proceed).await?;
    assert_eq!(left.batches(), ["us.1", "us.1"], "inputs start over");

    // The ledger is no longer empty: no need to proceed again.
    let again = common::route(
        "fresh",
        state.path(),
        vec![input([1, 2], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(again).await?;
    assert_eq!(left.batches(), ["us.1", "us.1", "us.2"]);
    Ok(())
}
