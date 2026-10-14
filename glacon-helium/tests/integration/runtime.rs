//! Running routes: graceful shutdown, failures, restarts, decode errors and
//! metrics.

use crate::common;
use glacon_helium::{
    FileStoreInput, IcebergOutput, Route, metrics as m,
    task_manager::{ManagedTask, TaskManager},
};

#[tokio::test]
async fn shutdown_commits_rows_still_in_the_spool() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    let key = h
        .put_file(
            b,
            common::V1_PREFIX,
            common::minutes_ago(30),
            vec![common::v1("a", "a", 1), common::v1("b", "a", 1)],
        )
        .await?;

    // Never stops on its own, and would not commit for an hour.
    let mut input = h.input_settings(b, common::start_after());
    input.idle_timeout = None;
    let mut output = h.output_settings();
    output.batch_timeout = humantime::parse_duration("1 hour")?;
    let route = Route::builder("shutdown", h.route_settings("shutdown"))
        .input("v1", FileStoreInput::new(common::V1Input, input))
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("events"), output),
        )
        .build()?;

    // Opened (created and migrated) before the route opens it too: two
    // pools racing to create the same state.db can find it locked.
    let ledger = h.ledger("shutdown").await?;
    let (trigger, shutdown) = triggered::trigger();
    let running = tokio::spawn(Box::new(route).start_task(shutdown));

    // Wait until the file is done: its rows are in the spool.
    for _ in 0..100 {
        if ledger.is_done("v1", &key).await? {
            break;
        }
        tokio::time::sleep(humantime::parse_duration("100ms")?).await;
    }
    assert!(ledger.is_done("v1", &key).await?);
    assert_eq!(common::count(&h, "events").await?, 0, "not committed yet");

    trigger.trigger();
    tokio::time::timeout(humantime::parse_duration("1 minute")?, running).await???;

    assert_eq!(common::count(&h, "events").await?, 2);
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_failing_route_stops_every_route() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;

    // Healthy and never ending on its own.
    let mut forever = h.input_settings(b, common::start_after());
    forever.idle_timeout = None;
    let healthy = Route::builder("healthy", h.route_settings("healthy"))
        .input("v1", FileStoreInput::new(common::V1Input, forever))
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("healthy_events"), h.output_settings()),
        )
        .build()?;

    // Its bucket does not exist, so its poller fails.
    let mut missing = h.input_settings(b, common::start_after());
    missing.bucket.bucket = "glacon-no-such-bucket".to_string();
    let broken = Route::builder("broken", h.route_settings("broken"))
        .input("v1", FileStoreInput::new(common::V1Input, missing))
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("broken_events"), h.output_settings()),
        )
        .build()?;

    let result = common::run(
        TaskManager::builder()
            .add_task(healthy)
            .add_task(broken)
            .build(),
    )
    .await;
    let err = format!(
        "{:#}",
        result.expect_err("the broken route must fail the run")
    );
    assert!(err.contains("broken"), "{err}");
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_restart_with_the_same_state_skips_files_already_read() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![common::v1("first", "a", 1)],
    )
    .await?;

    let route = |h: &common::Harness| {
        Route::builder("restart", h.route_settings("restart"))
            .input(
                "v1",
                FileStoreInput::new(common::V1Input, h.input_settings(b, common::start_after())),
            )
            .output(
                common::EVENTS,
                IcebergOutput::new(common::EventsTable("events"), h.output_settings()),
            )
            .build()
    };
    common::run(route(&h)?).await?;

    // Same state dir: the first file is not read again, only the new one.
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(10),
        vec![common::v1("second", "a", 1)],
    )
    .await?;
    common::run(route(&h)?).await?;

    assert_eq!(
        common::ids(&common::events(&h, "events").await?),
        ["first", "second"]
    );
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn undecodable_messages_are_dropped_and_counted() -> anyhow::Result<()> {
    common::metrics::install()?;
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![
            common::v1("ok", "a", 1),
            common::v1("bad1", "???", 1),
            common::v1("bad2", "", 1),
        ],
    )
    .await?;

    let route = Route::builder("decode_errors", h.route_settings("decode_errors"))
        .input(
            "v1",
            FileStoreInput::new(common::V1Input, h.input_settings(b, common::start_after())),
        )
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("events"), h.output_settings()),
        )
        .build()?;
    common::run(route).await?;

    assert_eq!(common::ids(&common::events(&h, "events").await?), ["ok"]);
    let labels = [("route", "decode_errors"), ("input", "v1")];
    assert_eq!(common::metrics::counter(m::DECODE_FAILURES, &labels)?, 2);
    assert_eq!(common::metrics::counter(m::RECORDS_DECODED, &labels)?, 1);
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn metrics_describe_the_route() -> anyhow::Result<()> {
    common::metrics::install()?;
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    for i in 0..3 {
        h.put_file(
            b,
            common::V1_PREFIX,
            common::minutes_ago(30 - i),
            vec![common::v1(&format!("e{i}"), "both", 1)],
        )
        .await?;
    }

    let route = Route::builder("metered", h.route_settings("metered"))
        .input(
            "v1",
            FileStoreInput::new(common::V1Input, h.input_settings(b, common::start_after())),
        )
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("events"), h.output_settings()),
        )
        .output(
            common::VALUES,
            IcebergOutput::new(common::ValuesTable("event_values"), h.output_settings()),
        )
        .build()?;
    common::run(route).await?;

    let input = [("route", "metered"), ("input", "v1")];
    assert_eq!(common::metrics::counter(m::PROCESSED_BATCHES, &input)?, 3);
    assert_eq!(common::metrics::counter(m::RECORDS_DECODED, &input)?, 3);
    assert!(common::metrics::gauge_exists(m::BATCH_LAG, &input)?);
    for output in [common::EVENTS, common::VALUES] {
        let labels = [("route", "metered"), ("output", output)];
        assert_eq!(
            common::metrics::counter(m::OUTPUT_RECORDS, &labels)?,
            3,
            "{output}"
        );
        assert_eq!(
            common::metrics::counter(m::OUTPUT_ROWS, &labels)?,
            3,
            "{output}"
        );
    }
    h.cleanup().await?;
    Ok(())
}
