//! The route's SQLite state: resuming, late files and the fresh-state guard.

use crate::common;
use glacon::{Error, FreshState, Glacon, Route, state::StateDb};

#[tokio::test]
async fn restart_resumes_without_reprocessing() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![common::v1("first", "a", 1)],
    )
    .await?;
    common::run(Glacon::new().route(route_builder(&h, "restart", b, "events").build()?)).await?;
    assert_eq!(common::ids(&common::events(&h, "events").await?), ["first"]);

    // Same state dir: the first file is not read again, only the new one.
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(10),
        vec![common::v1("second", "a", 1)],
    )
    .await?;
    common::run(Glacon::new().route(route_builder(&h, "restart", b, "events").build()?)).await?;
    assert_eq!(
        common::ids(&common::events(&h, "events").await?),
        ["first", "second"]
    );
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn late_file_inside_the_offset_window_is_picked_up() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(5),
        vec![common::v1("on_time", "a", 1)],
    )
    .await?;
    common::run(Glacon::new().route(route_builder(&h, "late", b, "events").build()?)).await?;

    // Older than the newest processed file, but within the poller's 10 minute
    // offset, so it is still listed on the next run.
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(8),
        vec![common::v1("late", "a", 1)],
    )
    .await?;
    common::run(Glacon::new().route(route_builder(&h, "late", b, "events").build()?)).await?;

    assert_eq!(
        common::ids(&common::events(&h, "events").await?),
        ["late", "on_time"]
    );
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn state_db_records_each_processed_file() -> anyhow::Result<()> {
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
    common::run(Glacon::new().route(route_builder(&h, "statedb", b, "events").build()?)).await?;

    let db = StateDb::open(
        &h.state_dir("statedb"),
        humantime::parse_duration("1 minute")?,
    )
    .await?;
    assert!(!db.is_fresh().await?);
    // Files are recorded even when every record was dropped.
    assert!(db.is_file_processed("v1", &first).await?);
    assert!(db.is_file_processed("v1", &second).await?);
    assert_eq!(
        db.latest_file_timestamp("v1")
            .await?
            .map(|t| t.timestamp_millis()),
        Some(ts.timestamp_millis())
    );
    assert!(db.in_flight_deliveries("v1", &first).await?.is_empty());
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn fresh_state_is_refused_when_a_table_has_data() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![common::v1("a", "a", 1)],
    )
    .await?;
    common::run(Glacon::new().route(route_builder(&h, "writer", b, "events").build()?)).await?;

    // A second route with an empty state writing the same table, e.g. after
    // losing the volume.
    let Err(err) = route_builder(&h, "lost_state", b, "events")
        .build()?
        .setup()
        .await
    else {
        anyhow::bail!("the fresh-state guard must refuse");
    };
    assert!(
        matches!(&err, Error::FreshStateRefused { output, .. } if output == common::EVENTS),
        "{err}"
    );
    assert_eq!(common::count(&h, "events").await?, 1, "nothing was written");
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn fresh_state_can_be_allowed_explicitly() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![common::v1("a", "a", 1)],
    )
    .await?;
    common::run(Glacon::new().route(route_builder(&h, "writer", b, "events").build()?)).await?;

    let rebuilt = route_builder(&h, "rebuilt", b, "events")
        .fresh_state(FreshState::Proceed)
        .build()?;
    common::run(Glacon::new().route(rebuilt)).await?;

    // Starting over from start_after re-ingests the file: that is the
    // duplication the guard exists to prevent by default.
    assert_eq!(common::count(&h, "events").await?, 2);
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn new_tables_pass_the_guard() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    // A brand-new route on brand-new tables starts without any policy.
    route_builder(&h, "brand_new", b, "events")
        .build()?
        .setup()
        .await?;
    h.cleanup().await?;
    Ok(())
}

fn route_builder(
    h: &common::Harness,
    name: &str,
    bucket: usize,
    table: &'static str,
) -> glacon::RouteBuilder<common::Event> {
    let mut route_builder = Route::builder(name, h.route_settings(name));
    route_builder.input(
        "v1",
        common::V1Input,
        h.input_settings(bucket, common::start_after()),
    );
    route_builder.output(
        common::EVENTS,
        common::EventsTable(table),
        h.output_settings(),
    );
    route_builder
}
