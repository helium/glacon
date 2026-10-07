//! Dedup is per route, across inputs, on `GlaconRecord::dedup_key`.

use crate::common;
use glacon::{Glacon, Route, metrics as m};
use glacon_test_proto::event_v2::Kind as V2Kind;

#[tokio::test]
async fn drops_duplicates_within_a_file() -> anyhow::Result<()> {
    common::metrics::install()?;
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![
            common::v1("a", "a", 1),
            common::v1("a", "a", 2),
            common::v1("b", "a", 3),
        ],
    )
    .await?;

    common::run(Glacon::new().route(route(&h, "dedup_in_file", &[b])?)).await?;

    let rows = common::events(&h, "events").await?;
    assert_eq!(common::ids(&rows), ["a", "b"]);
    assert_eq!(rows[0].value_sum, 1, "the first occurrence is kept");
    assert_eq!(
        common::metrics::counter(
            m::DEDUP_DROPPED,
            &[("route", "dedup_in_file"), ("input", "v1")]
        )?,
        1
    );
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn drops_duplicates_across_files() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![common::v1("a", "a", 1)],
    )
    .await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(20),
        vec![common::v1("a", "a", 2), common::v1("b", "a", 3)],
    )
    .await?;

    common::run(Glacon::new().route(route(&h, "dedup_files", &[b])?)).await?;

    assert_eq!(
        common::ids(&common::events(&h, "events").await?),
        ["a", "b"]
    );
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn drops_duplicates_across_inputs() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let (b1, b2) = (h.add_bucket().await?, h.add_bucket().await?);
    h.put_file(
        b1,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![common::v1("shared", "a", 1), common::v1("v1_only", "a", 1)],
    )
    .await?;
    h.put_file(
        b2,
        common::V2_PREFIX,
        common::minutes_ago(20),
        vec![
            common::v2("shared", V2Kind::A, &[2]),
            common::v2("v2_only", V2Kind::A, &[2]),
        ],
    )
    .await?;

    common::run(Glacon::new().route(route(&h, "dedup_inputs", &[b1, b2])?)).await?;

    assert_eq!(
        common::ids(&common::events(&h, "events").await?),
        ["shared", "v1_only", "v2_only"]
    );
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn records_without_a_key_are_never_dropped() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![common::v1("", "a", 1), common::v1("", "a", 1)],
    )
    .await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(20),
        vec![common::v1("", "a", 1)],
    )
    .await?;

    common::run(Glacon::new().route(route(&h, "dedup_no_key", &[b])?)).await?;

    assert_eq!(common::count(&h, "events").await?, 3);
    h.cleanup().await?;
    Ok(())
}

fn route(
    h: &common::Harness,
    name: &str,
    buckets: &[usize],
) -> glacon::Result<Route<common::Event>> {
    let mut route_builder = Route::builder(name, h.route_settings(name));
    for (i, b) in buckets.iter().enumerate() {
        if i == 0 {
            route_builder.input(
                "v1",
                common::V1Input,
                h.input_settings(*b, common::start_after()),
            );
        } else {
            route_builder.input(
                "v2",
                common::V2Input,
                h.input_settings(*b, common::start_after()),
            );
        }
    }
    route_builder.output(
        common::EVENTS,
        common::EventsTable("events"),
        h.output_settings(),
    );
    route_builder.build()
}
