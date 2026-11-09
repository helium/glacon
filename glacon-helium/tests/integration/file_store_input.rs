//! `FileStoreInput`: polling a bucket, decoding its files, resuming from the
//! route's ledger, and the poller failing.

use crate::common;
use glacon_helium::{
    FileStoreInput, IcebergOutput, Route, metrics as m, task_manager::TaskManager,
};
use glacon_test_proto::event_v2::Kind as V2Kind;

// ---------------------------------------------------------------- decoding

#[tokio::test]
async fn fan_in_from_inputs_with_different_protos() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let (b1, b2) = (h.add_bucket().await?, h.add_bucket().await?);
    h.put_file(
        b1,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![common::v1("a1", "a", 5), common::v1("a2", "a", 6)],
    )
    .await?;
    h.put_file(
        b2,
        common::V2_PREFIX,
        common::minutes_ago(20),
        vec![common::v2("b1", V2Kind::A, &[1, 2, 3])],
    )
    .await?;

    let route = Route::builder("events", h.route_settings("events"))
        .input(
            "v1",
            FileStoreInput::new(common::V1Input, h.input_settings(b1, common::start_after())),
        )
        .input(
            "v2",
            FileStoreInput::new(common::V2Input, h.input_settings(b2, common::start_after())),
        )
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("events"), h.output_settings()),
        )
        .build()?;
    common::run(route).await?;

    let rows = common::events(&h, "events").await?;
    let got: Vec<_> = rows
        .iter()
        .map(|e| (e.id.as_str(), e.source.as_str(), e.value_sum))
        .collect();
    assert_eq!(got, [("a1", "v1", 5), ("a2", "v1", 6), ("b1", "v2", 6)]);
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
async fn input_metrics_count_batches_and_records() -> anyhow::Result<()> {
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
        .build()?;
    common::run(route).await?;

    let input = [("route", "metered"), ("input", "v1")];
    assert_eq!(common::metrics::counter(m::PROCESSED_BATCHES, &input)?, 3);
    assert_eq!(common::metrics::counter(m::RECORDS_DECODED, &input)?, 3);
    assert!(common::metrics::gauge_exists(m::BATCH_LAG, &input)?);
    h.cleanup().await?;
    Ok(())
}

// ---------------------------------------------------------------- resuming from the ledger

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
async fn a_late_file_inside_the_offset_window_is_picked_up() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(5),
        vec![common::v1("on_time", "a", 1)],
    )
    .await?;
    common::run(common::route_builder(&h, "late", b, "events").build()?).await?;

    // Older than the newest done file, but within the poller's 10 minute
    // offset, so it is still listed on the next run.
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(8),
        vec![common::v1("late", "a", 1)],
    )
    .await?;
    common::run(common::route_builder(&h, "late", b, "events").build()?).await?;

    assert_eq!(
        common::ids(&common::events(&h, "events").await?),
        ["late", "on_time"]
    );
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn the_ledger_records_each_file_done() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    let first = h
        .put_file(
            b,
            common::V1_PREFIX,
            common::minutes_ago(30),
            vec![common::v1("a", "a", 1)],
        )
        .await?;
    let ts = common::minutes_ago(20);
    let second = h
        .put_file(b, common::V1_PREFIX, ts, vec![common::v1("b", "drop", 1)])
        .await?;
    // The middleware rejects "drop" records: the second file only gives an
    // error, which neither table keeps.
    let route = common::input_builder(&h, "ledger", b)
        .middleware(common::RejectDrop)
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

    let ledger = h.ledger("ledger").await?;
    assert!(!ledger.is_fresh().await?);
    // Done even when every record was rejected.
    assert!(ledger.is_done("v1", &first).await?);
    assert!(ledger.is_done("v1", &second).await?);
    assert_eq!(ledger.in_flight("v1").await?, 0);
    assert_eq!(
        ledger
            .resume_point("v1")
            .await?
            .map(|t| t.timestamp_millis()),
        Some(ts.timestamp_millis())
    );
    h.cleanup().await?;
    Ok(())
}

// ---------------------------------------------------------------- failures

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
