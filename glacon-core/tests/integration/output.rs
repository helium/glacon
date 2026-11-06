//! Outputs: every output acknowledges every batch, even one it keeps nothing
//! of; a failing output fails the route and gets the batch again alone on
//! restart; `already_written` closes the duplicate window; an output holding
//! data with an empty ledger is refused.

use crate::common::{self, End, Journal, MemoryInput, batch, event};
use glacon_core::{Error, FreshState};

// ---------------------------------------------------------------- acknowledging

#[tokio::test]
async fn an_output_that_keeps_nothing_still_acknowledges() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
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
    common::assert_nothing_in_flight(state.path(), "nothing").await
}

// ---------------------------------------------------------------- failures

#[tokio::test]
async fn a_failing_output_fails_the_route() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    right.fail_writes(1);
    let input = MemoryInput::new(
        "us",
        vec![batch("us.1", vec![event("x", "both")])],
        End::HoldOpen,
        &journal,
    );

    let route =
        common::route("output", state.path(), vec![input], &left, &right, &journal).build()?;
    let err = common::run(route).await.expect_err("must fail");

    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Output { output, .. } if output == common::RIGHT),
        "{err}"
    );
    assert!(journal.contains("terminate output left"));
    assert!(journal.contains("terminate output right"));
    assert!(journal.contains("drop input us"));
    Ok(())
}

#[tokio::test]
async fn a_failed_output_init_starts_nothing_else() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (mut left, right) = common::outputs(&journal);
    left.fail_init = true;
    let input = MemoryInput::new("us", Vec::new(), End::Return, &journal);

    let route = common::route(
        "output_init",
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
        matches!(err, Error::Output { output, .. } if output == common::LEFT),
        "{err}"
    );
    assert!(journal.positions("init middleware").is_empty());
    assert!(journal.positions("init input").is_empty());
    assert!(journal.contains("terminate output right"));
    Ok(())
}

// ---------------------------------------------------------------- recovery

#[tokio::test]
async fn a_failed_output_gets_the_batch_again_alone() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    right.fail_writes(1);

    let route = common::route(
        "retry",
        state.path(),
        vec![common::us_input([1], End::Return, &journal)],
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
        vec![common::us_input([1], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route).await?;

    assert_eq!(left.batches(), ["us.1"], "left written once");
    assert_eq!(right.batches(), ["us.1"]);
    common::assert_nothing_in_flight(state.path(), "retry").await
}

#[tokio::test]
async fn a_crash_after_a_write_duplicates_it_without_already_written() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    right.fail_after_writes(1);

    for _ in 0..2 {
        let route = common::route(
            "duplicate",
            state.path(),
            vec![common::us_input([1], End::Return, &journal)],
            &left,
            &right,
            &journal,
        )
        .build()?;
        let _ = common::run(route).await;
    }

    assert_eq!(left.batches(), ["us.1"]);
    assert_eq!(right.batches(), ["us.1", "us.1"], "the accepted window");
    common::assert_nothing_in_flight(state.path(), "duplicate").await
}

#[tokio::test]
async fn already_written_closes_the_duplicate_window() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, mut right) = common::outputs(&journal);
    right.knows_written = true;
    right.fail_after_writes(1);

    let route = common::route(
        "written",
        state.path(),
        vec![common::us_input([1], End::Return, &journal)],
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
        vec![common::us_input([1], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route).await?;

    assert_eq!(left.batches(), ["us.1"]);
    assert_eq!(right.batches(), ["us.1"]);
    common::assert_nothing_in_flight(state.path(), "written").await
}

// ---------------------------------------------------------------- existing data

#[tokio::test]
async fn an_empty_ledger_refuses_outputs_with_data() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    // Data written by a route with another ledger (say, a lost volume).
    let seed = common::route(
        "seed",
        state.path(),
        vec![common::us_input([1], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(seed).await?;

    let refused = common::route(
        "fresh",
        state.path(),
        vec![common::us_input([1], End::Return, &journal)],
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
        vec![common::us_input([1], End::Return, &journal)],
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
        vec![common::us_input([1, 2], End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(again).await?;
    assert_eq!(left.batches(), ["us.1", "us.1", "us.2"]);
    Ok(())
}
