//! Up and down: outputs, then middlewares, then inputs are initialized;
//! inputs, then middlewares, then outputs are terminated, whatever stops the
//! route.

use crate::common::{self, End, Journal, MemoryInput, MemoryOutput, batch, event};
use glacon_core::{Error, task_manager::TaskManager};

fn outputs(journal: &Journal) -> (MemoryOutput, MemoryOutput) {
    (
        MemoryOutput::new(common::LEFT, journal),
        MemoryOutput::new(common::RIGHT, journal),
    )
}

/// Every stage terminates after everything upstream of it: an input before
/// its middleware, every middleware before any output, and every write of an
/// output before it terminates.
fn assert_down_order(journal: &Journal) {
    let entries = journal.entries();
    let position = |entry: &str| entries.iter().position(|e| e == entry);
    for entry in &entries {
        if let Some(input) = entry.strip_prefix("terminate input ") {
            let middleware = position(&format!("terminate middleware {input}"));
            assert!(
                middleware.is_some_and(|m| Some(m) > position(entry)),
                "middleware of {input} not terminated after it: {entries:?}"
            );
        }
    }
    journal.assert_before("terminate middleware", "terminate output");
    for output in [common::LEFT, common::RIGHT] {
        let terminated = journal.positions(&format!("terminate output {output}"));
        let writes = journal.positions(&format!("write {output}"));
        assert!(
            writes.iter().all(|w| terminated.iter().all(|t| w < t)),
            "{output} wrote after terminating: {entries:?}"
        );
    }
}

#[tokio::test]
async fn up_initializes_outputs_then_middlewares_then_inputs() -> anyhow::Result<()> {
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    let inputs = ["a", "b"].map(|name| MemoryInput::new(name, Vec::new(), End::Return, &journal));

    let route = common::route("up", inputs.into(), &left, &right, &journal).build()?;
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
async fn ends_once_every_input_is_done() -> anyhow::Result<()> {
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    let inputs = ["a", "b"].map(|name| {
        MemoryInput::new(
            name,
            vec![batch(name, vec![event(name, "both")])],
            End::Return,
            &journal,
        )
    });

    let route = common::route("ends", inputs.into(), &left, &right, &journal).build()?;
    common::run(route).await?;

    assert_eq!(left.rows(), ["a", "b"]);
    assert_eq!(right.rows(), ["a", "b"]);
    assert_down_order(&journal);
    Ok(())
}

#[tokio::test]
async fn shutdown_stops_inputs_then_processors_then_outputs() -> anyhow::Result<()> {
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    let forever = MemoryInput::new(
        "forever",
        vec![
            batch("f.1", vec![event("x", "left")]),
            batch("f.2", vec![event("y", "left")]),
        ],
        End::HoldOpen,
        &journal,
    );

    let route = common::route("shutdown", vec![forever], &left, &right, &journal).build()?;
    common::run_until(route, left.wait_for_rows(2)).await?;

    assert_eq!(left.rows(), ["x", "y"]);
    assert_down_order(&journal);
    Ok(())
}

#[tokio::test]
async fn a_failing_input_fails_the_route() -> anyhow::Result<()> {
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    let healthy = MemoryInput::new("healthy", Vec::new(), End::HoldOpen, &journal);
    let broken = MemoryInput::new(
        "broken",
        vec![batch("b.1", vec![event("kept", "left")])],
        End::Fail,
        &journal,
    );

    let route = common::route("inputs", vec![healthy, broken], &left, &right, &journal).build()?;
    let err = common::run(route).await.expect_err("must fail");

    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Input { route, input, .. } if route == "inputs" && input == "broken"),
        "{err}"
    );
    assert_eq!(left.rows(), ["kept"], "what was read before still got out");
    assert!(journal.contains("terminate input healthy"));
    assert!(journal.contains("terminate input broken"));
    assert_down_order(&journal);
    Ok(())
}

#[tokio::test]
async fn a_failing_middleware_fails_the_route() -> anyhow::Result<()> {
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    let input = MemoryInput::new(
        "us",
        vec![batch("us.1", vec![event("x", "fail")])],
        End::HoldOpen,
        &journal,
    );

    let route = common::route("middleware", vec![input], &left, &right, &journal).build()?;
    let err = common::run(route).await.expect_err("must fail");

    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Middleware { input, .. } if input == "us"),
        "{err}"
    );
    assert_down_order(&journal);
    Ok(())
}

#[tokio::test]
async fn a_failing_output_fails_the_route() -> anyhow::Result<()> {
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    right.fail_writes(1);
    let input = MemoryInput::new(
        "us",
        vec![batch("us.1", vec![event("x", "both")])],
        End::HoldOpen,
        &journal,
    );

    let route = common::route("output", vec![input], &left, &right, &journal).build()?;
    let err = common::run(route).await.expect_err("must fail");

    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Output { output, .. } if output == common::RIGHT),
        "{err}"
    );
    assert!(journal.contains("terminate output left"));
    assert!(journal.contains("terminate output right"));
    assert!(journal.contains("terminate input us"));
    Ok(())
}

#[tokio::test]
async fn a_panic_fails_the_route() -> anyhow::Result<()> {
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    let input = MemoryInput::new("panicking", Vec::new(), End::Panic, &journal);

    let route = common::route("panics", vec![input], &left, &right, &journal).build()?;
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
async fn a_failed_init_terminates_what_was_initialized() -> anyhow::Result<()> {
    let journal = Journal::default();
    let (left, right) = outputs(&journal);
    let mut broken = MemoryInput::new("broken", Vec::new(), End::Return, &journal);
    broken.fail_init = true;
    let healthy = MemoryInput::new("healthy", Vec::new(), End::Return, &journal);

    let route = common::route("init", vec![healthy, broken], &left, &right, &journal).build()?;
    let err = common::run(route).await.expect_err("must fail");

    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Input { input, .. } if input == "broken"),
        "{err}"
    );
    // The healthy input, every middleware and every output are undone, in
    // Down order.
    assert_down_order(&journal);
    assert!(journal.contains("terminate input healthy"));
    assert!(
        !journal.contains("terminate input broken"),
        "never initialized"
    );
    assert_eq!(journal.positions("terminate middleware").len(), 2);
    Ok(())
}

#[tokio::test]
async fn a_failed_output_init_starts_nothing_else() -> anyhow::Result<()> {
    let journal = Journal::default();
    let (mut left, right) = outputs(&journal);
    left.fail_init = true;
    let input = MemoryInput::new("us", Vec::new(), End::Return, &journal);

    let route = common::route("output_init", vec![input], &left, &right, &journal).build()?;
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

#[tokio::test]
async fn a_failing_route_stops_the_others_in_the_task_manager() -> anyhow::Result<()> {
    let journal = Journal::default();
    let healthy_outputs = (
        MemoryOutput::new(common::LEFT, &journal),
        MemoryOutput::new(common::RIGHT, &journal),
    );
    let healthy = common::route(
        "healthy",
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
    let (left, right) = outputs(&Journal::default());
    let failing = common::route(
        "failing",
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
    assert!(journal.contains("terminate input forever"));
    assert_down_order(&journal);
    Ok(())
}
