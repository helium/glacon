//! `IcebergOutput`: rows made from records, one table or several, spooling
//! and committing, existing data, and a crash mid-run.

use crate::common;
use glacon_helium::{
    FileStoreInput, FreshState, IcebergOutput, InputSettings, OutputSettings, Route, RouteSettings,
    metrics as m,
    task_manager::{ManagedTask, TaskManager},
};
use glacon_test_proto::event_v2::Kind as V2Kind;
use serde_json::{Value, json};
use std::{collections::BTreeSet, process::Stdio};

// ---------------------------------------------------------------- rows

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

#[tokio::test]
async fn output_metrics_count_records_handed_and_rows_kept() -> anyhow::Result<()> {
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

// ---------------------------------------------------------------- spool and commits

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

// ---------------------------------------------------------------- existing data

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
    common::run(common::route_builder(&h, "writer", b, "events").build()?).await?;

    // A second route with an empty ledger writing the same table, e.g. after
    // losing the volume.
    let err = common::run(common::route_builder(&h, "lost_state", b, "events").build()?)
        .await
        .expect_err("the fresh-state guard must refuse");
    let err = format!("{err:#}");
    assert!(err.contains("output events already has data"), "{err}");
    assert_eq!(common::count(&h, "events").await?, 1, "nothing was written");

    // Proceeding starts over from start_after and re-ingests the file: the
    // duplication the guard prevents by default.
    let rebuilt = common::route_builder(&h, "lost_state", b, "events")
        .fresh_state(FreshState::Proceed)
        .build()?;
    common::run(rebuilt).await?;
    assert_eq!(common::count(&h, "events").await?, 2);
    h.cleanup().await?;
    Ok(())
}

// ---------------------------------------------------------------- crash

/// Set in the crash child's environment: the settings of its route, as JSON.
const CHILD_ENV: &str = "GLACON_CRASH_CHILD";
const CRASH_ROUTE: &str = "crash";
const FILES: usize = 60;
const RECORDS_PER_FILE: usize = 20;

/// The route the crash test kills and restarts.
fn crash_route(
    route_settings: RouteSettings,
    input: InputSettings,
    output: OutputSettings,
) -> anyhow::Result<Route<common::Event>> {
    Ok(Route::builder(CRASH_ROUTE, route_settings)
        .input("v1", FileStoreInput::new(common::V1Input, input))
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("events"), output),
        )
        .build()?)
}

/// The route of the crash test's child process, until it is killed. Does
/// nothing when run on its own.
#[tokio::test]
#[ignore = "run by iceberg_output::a_killed_route_loses_nothing_on_restart"]
async fn crash_child() -> anyhow::Result<()> {
    let Ok(settings) = std::env::var(CHILD_ENV) else {
        return Ok(());
    };
    let settings: Value = serde_json::from_str(&settings)?;
    let route = crash_route(
        serde_json::from_value(settings["route"].clone())?,
        serde_json::from_value(settings["input"].clone())?,
        serde_json::from_value(settings["output"].clone())?,
    )?;
    let (_trigger, shutdown) = triggered::trigger();
    Box::new(route).start_task(shutdown).await?;
    Ok(())
}

/// A real crash: the route runs in a child process (this test binary, running
/// [`crash_child`]), killed with SIGKILL mid-run. Restarted on the same
/// state and spool dirs, it loses nothing. It may duplicate, at most:
/// - the file the output spooled right before the kill, not yet recorded in
///   the ledger (glacon's accepted window);
/// - one commit's worth of rows: `helium_iceberg` commits a spool file, then
///   removes it, and replays a leftover spool file without checking whether
///   it was committed.
#[tokio::test]
async fn a_killed_route_loses_nothing_on_restart() -> anyhow::Result<()> {
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    let mut keys = Vec::with_capacity(FILES);
    for file in 0..FILES {
        let events = (0..RECORDS_PER_FILE)
            .map(|i| common::v1(&format!("f{file:02}-{i:02}"), "a", 1))
            .collect();
        let at = common::minutes_ago(50) + chrono::Duration::seconds(file as i64);
        keys.push(h.put_file(b, common::V1_PREFIX, at, events).await?);
    }

    // The child never stops on its own and commits every few files, so the
    // kill leaves rows both committed and only spooled.
    let mut input = h.input_settings(b, common::start_after());
    input.idle_timeout = None;
    let mut output = h.output_settings();
    output.max_batch_rows = 3 * RECORDS_PER_FILE;
    output.batch_timeout = humantime::parse_duration("1 hour")?;
    let settings = json!({
        "route": { "state_dir": h.route_settings(CRASH_ROUTE).state_dir },
        "input": input_json(&input),
        "output": output_json(&output)?,
    });

    // Created before the child opens it too (see the shutdown test).
    let ledger = h.ledger(CRASH_ROUTE).await?;
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args(["iceberg_output::crash_child", "--exact", "--ignored"])
        .env(CHILD_ENV, settings.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    // Kill once a few files are done, well before all of them.
    let mut done = 0;
    for _ in 0..600 {
        done = 0;
        for key in &keys {
            done += usize::from(ledger.is_done("v1", key).await?);
        }
        if done >= 5 {
            break;
        }
        tokio::time::sleep(humantime::parse_duration("50ms")?).await;
    }
    // SIGKILL: no shutdown, nothing drained.
    child.kill()?;
    child.wait()?;
    assert!(done >= 5, "the child never got going");

    // Restart on the same state and spool dirs.
    common::run(crash_route(
        h.route_settings(CRASH_ROUTE),
        h.input_settings(b, common::start_after()),
        h.output_settings(),
    )?)
    .await?;

    let ids: Vec<String> = common::events(&h, "events")
        .await?
        .into_iter()
        .map(|e| e.id)
        .collect();
    let distinct: BTreeSet<&String> = ids.iter().collect();
    assert_eq!(distinct.len(), FILES * RECORDS_PER_FILE, "nothing lost");
    let extra = ids.len() - distinct.len();
    assert!(
        extra <= RECORDS_PER_FILE + 3 * RECORDS_PER_FILE,
        "at most one file and one commit duplicated, got {extra} extra rows"
    );
    assert_eq!(ledger.in_flight("v1").await?, 0);
    for key in &keys {
        assert!(ledger.is_done("v1", key).await?, "{key}");
    }
    h.cleanup().await?;
    Ok(())
}

/// `InputSettings` as JSON, credentials included (file_store skips them
/// when serializing).
fn input_json(s: &InputSettings) -> Value {
    let bucket = &s.bucket.settings;
    json!({
        "bucket": {
            "bucket": s.bucket.bucket,
            "region": bucket.region,
            "endpoint": bucket.endpoint,
            "access_key_id": bucket.access_key_id,
            "secret_access_key": bucket.secret_access_key,
        },
        "start_after": s.start_after.to_rfc3339(),
        "poll_duration": "100ms",
    })
}

/// `OutputSettings` as JSON, credentials included (helium_iceberg skips them
/// when serializing).
fn output_json(s: &OutputSettings) -> anyhow::Result<Value> {
    let mut catalog = serde_json::to_value(&s.catalog)?;
    catalog["auth"]["token"] = json!(s.catalog.auth.token);
    catalog["auth"]["credential"] = json!(s.catalog.auth.credential);
    catalog["s3"]["access_key_id"] = json!(s.catalog.s3.access_key_id);
    catalog["s3"]["secret_access_key"] = json!(s.catalog.s3.secret_access_key);
    Ok(json!({
        "catalog": catalog,
        "spool_dir": s.spool_dir,
        "max_batch_rows": s.max_batch_rows,
        "batch_timeout": humantime::format_duration(s.batch_timeout).to_string(),
    }))
}
