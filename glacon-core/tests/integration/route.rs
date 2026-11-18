//! The route as a whole. Up and down: outputs, then middlewares, then inputs
//! are initialized; inputs, then middlewares, then outputs stop, whatever
//! stops the route. Background tasks returned by `init` start before their
//! stage and stop after it. A failing route stops the others, and a restart
//! after shutdown loses and duplicates nothing.

use crate::common::{self, BackgroundEnd, End, Journal, MemoryInput, MemoryOutput, batch, event};
use glacon_core::{Error, task_manager::TaskManager};

// ---------------------------------------------------------------- up and down

#[tokio::test]
async fn up_initializes_outputs_then_middlewares_then_inputs() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let inputs = ["a", "b"].map(|name| MemoryInput::new(name, Vec::new(), End::Return, &journal));

    let route =
        common::route("up", state.path(), inputs.into(), &left, &right, &journal).build()?;
    common::run(route).await?;

    journal.assert_before("init output", "init middleware");
    journal.assert_before("init middleware", "init input");
    assert_eq!(
        journal.positions("init middleware").len(),
        2,
        "one per input"
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_stops_inputs_then_processors_then_outputs() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let forever = MemoryInput::new(
        "forever",
        vec![
            batch("f.1", vec![event("x", "left")]),
            batch("f.2", vec![event("y", "left")]),
        ],
        End::HoldOpen,
        &journal,
    );

    let route = common::route(
        "shutdown",
        state.path(),
        vec![forever],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run_until(route, left.wait_for_rows(2)).await?;

    assert_eq!(left.rows(), ["x", "y"]);
    common::assert_down_order(&journal);
    Ok(())
}

#[tokio::test]
async fn a_failed_init_terminates_what_was_initialized() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let mut broken = MemoryInput::new("broken", Vec::new(), End::Return, &journal);
    broken.fail_init = true;
    let mut healthy = MemoryInput::new("healthy", Vec::new(), End::Return, &journal);
    healthy.background = Some(BackgroundEnd::UntilStopped);

    let route = common::route(
        "init",
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
        matches!(err, Error::Input { input, .. } if input == "broken"),
        "{err}"
    );
    // The healthy input, every middleware and every output are undone, in
    // Down order; the background task the healthy input returned never ran.
    common::assert_down_order(&journal);
    assert!(journal.contains("drop input healthy"));
    assert!(
        journal.positions("start").is_empty(),
        "{:?}",
        journal.entries()
    );
    assert_eq!(journal.positions("terminate middleware").len(), 2);
    Ok(())
}

// ---------------------------------------------------------------- failures

#[tokio::test]
async fn a_panic_fails_the_route() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let input = MemoryInput::new("panicking", Vec::new(), End::Panic, &journal);

    let route =
        common::route("panics", state.path(), vec![input], &left, &right, &journal).build()?;
    let err = common::run(route).await.expect_err("must fail");

    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Panic { route, task, message }
            if route == "panics" && task == "input panicking" && message == "input panic"),
        "{err}"
    );
    // The other stages still went down in order.
    journal.assert_before("terminate middleware", "terminate output");
    Ok(())
}

#[tokio::test]
async fn a_failing_route_stops_the_others_in_the_task_manager() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let healthy_outputs = (
        MemoryOutput::new(common::LEFT, &journal),
        MemoryOutput::new(common::RIGHT, &journal),
    );
    let healthy = common::route(
        "healthy",
        state.path(),
        vec![MemoryInput::new(
            "forever",
            Vec::new(),
            End::HoldOpen,
            &journal,
        )],
        &healthy_outputs.0,
        &healthy_outputs.1,
        &journal,
    )
    .build()?;
    let (left, right) = common::outputs(&Journal::default());
    let failing = common::route(
        "failing",
        state.path(),
        vec![MemoryInput::new(
            "broken",
            Vec::new(),
            End::Fail,
            &Journal::default(),
        )],
        &left,
        &right,
        &Journal::default(),
    )
    .build()?;

    let routes = TaskManager::builder()
        .add_task(healthy)
        .add_task(failing)
        .build();
    let err = common::run(routes).await.expect_err("must fail");

    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Input { route, .. } if route == "failing"),
        "{err}"
    );
    // The healthy route went down gracefully.
    assert!(journal.contains("drop input forever"));
    common::assert_down_order(&journal);
    Ok(())
}

// ---------------------------------------------------------------- background tasks

#[tokio::test]
async fn background_tasks_stop_after_their_stage() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (mut left, right) = common::outputs(&journal);
    left.background = Some(BackgroundEnd::UntilStopped);
    let mut forever = MemoryInput::new(
        "forever",
        vec![batch("f.1", vec![event("x", "left")])],
        End::HoldOpen,
        &journal,
    );
    forever.background = Some(BackgroundEnd::UntilStopped);

    let route = common::route(
        "background",
        state.path(),
        vec![forever],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run_until(route, left.wait_for_rows(1)).await?;

    common::assert_down_order(&journal);
    journal.assert_before("start input forever", "stop input forever");
    journal.assert_before("drop input forever", "stop input forever");
    journal.assert_before("terminate output left", "stop output left");
    Ok(())
}

#[tokio::test]
async fn a_failing_background_task_fails_the_route() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    for side in ["input", "output"] {
        let journal = Journal::default();
        let (mut left, right) = common::outputs(&journal);
        let mut input = MemoryInput::new("us", Vec::new(), End::HoldOpen, &journal);
        match side {
            "input" => input.background = Some(BackgroundEnd::Fail),
            _ => left.background = Some(BackgroundEnd::Fail),
        }

        let route = common::route(
            "background",
            state.path(),
            vec![input],
            &left,
            &right,
            &journal,
        )
        .build()?;
        let err = common::run(route).await.expect_err("must fail");

        let err = common::route_error(&err);
        let named = match err {
            Error::Input { input, .. } => side == "input" && input == "us",
            Error::Output { output, .. } => side == "output" && output == common::LEFT,
            _ => false,
        };
        assert!(named, "{side}: {err}");
        assert!(err.to_string().contains("background failure"), "{err}");
        common::assert_down_order(&journal);
    }
    Ok(())
}

#[tokio::test]
async fn a_panicking_background_task_fails_the_route() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);
    let mut input = MemoryInput::new("us", Vec::new(), End::HoldOpen, &journal);
    input.background = Some(BackgroundEnd::Panic);

    let route = common::route(
        "background",
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
        matches!(err, Error::Panic { task, message, .. }
            if task == "input us: background" && message == "background panic"),
        "{err}"
    );
    common::assert_down_order(&journal);
    Ok(())
}

// ---------------------------------------------------------------- restart

#[tokio::test]
async fn a_restart_after_shutdown_loses_and_duplicates_nothing() -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let journal = Journal::default();
    let (left, right) = common::outputs(&journal);

    let route = common::route(
        "restart",
        state.path(),
        vec![common::us_input(1..=5, End::HoldOpen, &journal)],
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
        vec![common::us_input(1..=5, End::Return, &journal)],
        &left,
        &right,
        &journal,
    )
    .build()?;
    common::run(route).await?;

    let all = ["us.1", "us.2", "us.3", "us.4", "us.5"];
    assert_eq!(left.batches(), all);
    assert_eq!(right.batches(), all);
    common::assert_nothing_in_flight(state.path(), "restart").await
}
