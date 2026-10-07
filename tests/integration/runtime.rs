//! Running routes: graceful shutdown, failures, decode errors and metrics.

use crate::common;
use glacon::{Glacon, Route, metrics as m, task_manager::ManagedTask};

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
        .input("v1", common::V1Input, input)
        .output(common::EVENTS, common::EventsTable("events"), output)
        .build()?;

    let (trigger, shutdown) = triggered::trigger();
    let task_manager = Glacon::new().route(route).into_task_manager()?;
    let running = tokio::spawn(Box::new(task_manager).start_task(shutdown));

    // Wait until the file is processed (rows spooled, not committed).
    for _ in 0..100 {
        if common::is_file_processed(&h, "shutdown", "v1", &key).await? {
            break;
        }
        tokio::time::sleep(humantime::parse_duration("100ms")?).await;
    }
    assert!(common::is_file_processed(&h, "shutdown", "v1", &key).await?);
    assert_eq!(common::count(&h, "events").await?, 0, "still in the spool");

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
        .input("v1", common::V1Input, forever)
        .output(
            common::EVENTS,
            common::EventsTable("healthy_events"),
            h.output_settings(),
        )
        .build()?;

    // Its bucket does not exist, so its poller fails.
    let mut missing = h.input_settings(b, common::start_after());
    missing.bucket.bucket = "glacon-no-such-bucket".to_string();
    let broken = Route::builder("broken", h.route_settings("broken"))
        .input("v1", common::V1Input, missing)
        .output(
            common::EVENTS,
            common::EventsTable("broken_events"),
            h.output_settings(),
        )
        .build()?;

    let result = common::run(Glacon::new().route(healthy).route(broken)).await;
    let err = format!(
        "{:#}",
        result.expect_err("the broken route must fail the run")
    );
    assert!(err.contains("broken"), "{err}");
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
            common::V1Input,
            h.input_settings(b, common::start_after()),
        )
        .output(
            common::EVENTS,
            common::EventsTable("events"),
            h.output_settings(),
        )
        .build()?;
    common::run(Glacon::new().route(route)).await?;

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
            common::V1Input,
            h.input_settings(b, common::start_after()),
        )
        .output(
            common::EVENTS,
            common::EventsTable("events"),
            h.output_settings(),
        )
        .output(
            common::VALUES,
            common::ValuesTable("event_values"),
            h.output_settings(),
        )
        .build()?;
    common::run(Glacon::new().route(route)).await?;

    let input = [("route", "metered"), ("input", "v1")];
    assert_eq!(common::metrics::counter(m::PROCESSED_FILES, &input)?, 3);
    assert_eq!(common::metrics::counter(m::RECORDS_DECODED, &input)?, 3);
    assert!(common::metrics::gauge_exists(m::FILE_LAG, &input)?);
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
