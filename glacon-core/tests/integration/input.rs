//! Inputs: a route ends once every input is done, an input ends with its
//! background task, a failing input fails the route, and batches the ledger
//! has as done are not read again.

use crate::common::{self, ChannelInput, End, Journal, MemoryInput, batch, event};
use glacon_core::{Error, Route};

// ---------------------------------------------------------------- ending

#[tokio::test]
async fn ends_once_every_input_is_done() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let inputs = ["a", "b"].map(|name| {
        MemoryInput::new(
            name,
            vec![batch(name, vec![event(name, "both")])],
            End::Return,
            &journal,
        )
    });

    let route =
        common::route("ends", state.path(), inputs.into(), &left, &right, &journal).build()?;
    common::run(route).await?;

    assert_eq!(left.rows(), ["a", "b"]);
    assert_eq!(right.rows(), ["a", "b"]);
    common::assert_down_order(&journal);
    Ok(())
}

#[tokio::test]
async fn an_input_ends_when_its_background_task_does() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let feed = ChannelInput::new(
        "feed",
        vec![
            batch("feed.1", vec![event("a", "left")]),
            batch("feed.2", vec![event("b", "left")]),
        ],
        &journal,
    );

    let builder =
        Route::builder("channel", common::settings(state.path(), "channel")).input(feed.name, feed);
    let route = common::route_from(builder, &left, &right, &journal).build()?;
    common::run(route).await?;

    assert_eq!(left.rows(), ["a", "b"]);
    journal.assert_before("stop input feed", "terminate output");
    Ok(())
}

// ---------------------------------------------------------------- failures

#[tokio::test]
async fn a_failing_input_fails_the_route() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let healthy = MemoryInput::new("healthy", Vec::new(), End::HoldOpen, &journal);
    let broken = MemoryInput::new(
        "broken",
        vec![batch("b.1", vec![event("kept", "left")])],
        End::Fail,
        &journal,
    );

    let route = common::route(
        "inputs",
        state.path(),
        vec![healthy, broken],
        &left,
        &right,
        &journal,
    )
    .build()?;
    let err = common::run(route).await.expect_err("must fail");

    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Input { route, input, .. } if route == "inputs" && input == "broken"),
        "{err}"
    );
    assert_eq!(left.rows(), ["kept"], "what was read before still got out");
    assert!(journal.contains("drop input healthy"));
    assert!(journal.contains("drop input broken"));
    common::assert_down_order(&journal);
    Ok(())
}

// ---------------------------------------------------------------- ledger

#[tokio::test]
async fn done_batches_are_skipped() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);

    for batches in [1..=2, 1..=3] {
        let route = common::route(
            "skip",
            state.path(),
            vec![common::us_input(batches, End::Return, &journal)],
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
