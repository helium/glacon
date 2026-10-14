//! Inputs to outputs: 1 -> 1, N -> 1 (mixed protos), 1 -> M (different rows),
//! N -> M, and several routes in one TaskManager.

use crate::common;
use glacon_helium::{FileStoreInput, IcebergOutput, Route, task_manager::TaskManager};
use glacon_test_proto::event_v2::Kind as V2Kind;

#[tokio::test]
async fn one_input_to_one_output() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    for (i, id) in ["e1", "e2", "e3"].iter().enumerate() {
        h.put_file(
            b,
            common::V1_PREFIX,
            common::minutes_ago(30 - i as i64),
            vec![common::v1(id, "a", 1)],
        )
        .await?;
    }

    let route = Route::builder("events", h.route_settings("events"))
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

    assert_eq!(
        common::ids(&common::events(&h, "events").await?),
        ["e1", "e2", "e3"]
    );
    h.cleanup().await?;
    Ok(())
}

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
async fn fan_out_to_outputs_with_different_rows() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V2_PREFIX,
        common::minutes_ago(30),
        vec![
            common::v2("many", V2Kind::Both, &[7, 8, 9]),
            common::v2("none", V2Kind::Both, &[]),
        ],
    )
    .await?;

    let route = Route::builder("events", h.route_settings("events"))
        .input(
            "v2",
            FileStoreInput::new(common::V2Input, h.input_settings(b, common::start_after())),
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

    // Every event gets one row in `events`...
    assert_eq!(
        common::ids(&common::events(&h, "events").await?),
        ["many", "none"]
    );
    // ...and one row per value in `event_values` (none for "none").
    let got: Vec<_> = common::values(&h, "event_values")
        .await?
        .into_iter()
        .map(|v| (v.event_id, v.position, v.value))
        .collect();
    assert_eq!(
        got,
        [
            ("many".into(), 0, 7),
            ("many".into(), 1, 8),
            ("many".into(), 2, 9)
        ]
    );
    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn fan_in_and_fan_out_from_n_inputs_to_m_outputs() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let (b1, b2) = (h.add_bucket().await?, h.add_bucket().await?);
    let v1_files = [
        h.put_file(
            b1,
            common::V1_PREFIX,
            common::minutes_ago(30),
            vec![common::v1("a1", "a", 5), common::v1("a2", "a", 6)],
        )
        .await?,
        h.put_file(
            b1,
            common::V1_PREFIX,
            common::minutes_ago(25),
            vec![common::v1("a3", "a", 7)],
        )
        .await?,
    ];
    let v2_file = h
        .put_file(
            b2,
            common::V2_PREFIX,
            common::minutes_ago(20),
            vec![
                common::v2("b1", V2Kind::A, &[1, 2]),
                common::v2("b2", V2Kind::A, &[]),
            ],
        )
        .await?;

    // Default middleware: every record of every input goes to every output.
    let route = Route::builder("n_to_m", h.route_settings("n_to_m"))
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
        .output(
            common::VALUES,
            IcebergOutput::new(common::ValuesTable("event_values"), h.output_settings()),
        )
        .build()?;
    common::run(route).await?;

    // `events`: one row per record, from both inputs.
    let got: Vec<_> = common::events(&h, "events")
        .await?
        .into_iter()
        .map(|e| (e.id, e.source, e.value_sum))
        .collect();
    let expected: Vec<(String, String, i64)> = [
        ("a1", "v1", 5),
        ("a2", "v1", 6),
        ("a3", "v1", 7),
        ("b1", "v2", 3),
        ("b2", "v2", 0),
    ]
    .into_iter()
    .map(|(id, source, sum)| (id.into(), source.into(), sum))
    .collect();
    assert_eq!(got, expected);

    // `event_values`: one row per value, from both inputs ("b2" has none).
    let got: Vec<_> = common::values(&h, "event_values")
        .await?
        .into_iter()
        .map(|v| (v.event_id, v.position, v.value))
        .collect();
    let expected: Vec<(String, i32, i64)> = [
        ("a1", 0, 5),
        ("a2", 0, 6),
        ("a3", 0, 7),
        ("b1", 0, 1),
        ("b1", 1, 2),
    ]
    .into_iter()
    .map(|(id, position, value)| (id.into(), position, value))
    .collect();
    assert_eq!(got, expected);

    // Each input's files are done under that input.
    let ledger = h.ledger("n_to_m").await?;
    for file in &v1_files {
        assert!(ledger.is_done("v1", file).await?);
        assert!(!ledger.is_done("v2", file).await?);
    }
    assert!(ledger.is_done("v2", &v2_file).await?);

    h.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn several_routes_run_side_by_side() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![common::v1("x", "a", 1)],
    )
    .await?;
    h.put_file(
        b,
        common::V2_PREFIX,
        common::minutes_ago(30),
        vec![common::v2("y", V2Kind::A, &[2])],
    )
    .await?;

    let first = Route::builder("first", h.route_settings("first"))
        .input(
            "v1",
            FileStoreInput::new(common::V1Input, h.input_settings(b, common::start_after())),
        )
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("first_events"), h.output_settings()),
        )
        .build()?;
    let second = Route::builder("second", h.route_settings("second"))
        .input(
            "v2",
            FileStoreInput::new(common::V2Input, h.input_settings(b, common::start_after())),
        )
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("second_events"), h.output_settings()),
        )
        .build()?;
    common::run(
        TaskManager::builder()
            .add_task(first)
            .add_task(second)
            .build(),
    )
    .await?;

    assert_eq!(
        common::ids(&common::events(&h, "first_events").await?),
        ["x"]
    );
    assert_eq!(
        common::ids(&common::events(&h, "second_events").await?),
        ["y"]
    );
    h.cleanup().await?;
    Ok(())
}
