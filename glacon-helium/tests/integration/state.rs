//! The route's ledger as the poller's state: late files, what the ledger
//! records, and the fresh-state guard against tables that already hold data.

use crate::common;
use glacon_helium::{FileStoreInput, FreshState, IcebergOutput, Route, RouteBuilder, RouteOutputs};

/// Route `name` reading bucket `b`.
fn input_builder(h: &common::Harness, name: &str, b: usize) -> RouteBuilder<common::Event> {
    Route::builder(name, h.route_settings(name)).input(
        "v1",
        FileStoreInput::new(common::V1Input, h.input_settings(b, common::start_after())),
    )
}

/// Route `name` from bucket `b` into the events table `table`.
fn route_builder(
    h: &common::Harness,
    name: &str,
    b: usize,
    table: &'static str,
) -> RouteOutputs<common::Event, common::Event> {
    input_builder(h, name, b).output(
        common::EVENTS,
        IcebergOutput::new(common::EventsTable(table), h.output_settings()),
    )
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
    common::run(route_builder(&h, "late", b, "events").build()?).await?;

    // Older than the newest done file, but within the poller's 10 minute
    // offset, so it is still listed on the next run.
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(8),
        vec![common::v1("late", "a", 1)],
    )
    .await?;
    common::run(route_builder(&h, "late", b, "events").build()?).await?;

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
    // The middleware drops "drop" records: the second file goes nowhere.
    let route = input_builder(&h, "ledger", b)
        .middleware(common::DropKind)
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
    // Done even when every record was dropped.
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

#[tokio::test]
async fn an_empty_ledger_is_refused_when_a_table_has_data() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![common::v1("a", "a", 1)],
    )
    .await?;
    common::run(route_builder(&h, "writer", b, "events").build()?).await?;

    // A second route with an empty ledger writing the same table, e.g. after
    // losing the volume.
    let err = common::run(route_builder(&h, "lost_state", b, "events").build()?)
        .await
        .expect_err("the fresh-state guard must refuse");
    let err = format!("{err:#}");
    assert!(err.contains("output events already has data"), "{err}");
    assert_eq!(common::count(&h, "events").await?, 1, "nothing was written");

    // Proceeding starts over from start_after and re-ingests the file: the
    // duplication the guard prevents by default.
    let rebuilt = route_builder(&h, "lost_state", b, "events")
        .fresh_state(FreshState::Proceed)
        .build()?;
    common::run(rebuilt).await?;
    assert_eq!(common::count(&h, "events").await?, 2);
    h.cleanup().await?;
    Ok(())
}
