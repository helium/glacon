//! A real crash: the route runs in a child process (this test binary, running
//! [`crash_child`]), killed with SIGKILL mid-run. Restarted on the same
//! state and spool dirs, it loses nothing. It may duplicate, at most:
//! - the file the output spooled right before the kill, not yet recorded in
//!   the ledger (glacon's accepted window);
//! - one commit's worth of rows: `helium_iceberg` commits a spool file, then
//!   removes it, and replays a leftover spool file without checking whether
//!   it was committed.

use crate::common;
use glacon_helium::{
    FileStoreInput, IcebergOutput, InputSettings, OutputSettings, Route, RouteSettings,
    task_manager::ManagedTask,
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, process::Stdio};

/// Set in the child's environment: the settings of its route, as JSON.
const CHILD_ENV: &str = "GLACON_CRASH_CHILD";
const ROUTE: &str = "crash";
const FILES: usize = 60;
const RECORDS_PER_FILE: usize = 20;

fn route(
    route_settings: RouteSettings,
    input: InputSettings,
    output: OutputSettings,
) -> anyhow::Result<Route<common::Event>> {
    Ok(Route::builder(ROUTE, route_settings)
        .input("v1", FileStoreInput::new(common::V1Input, input))
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("events"), output),
        )
        .build()?)
}

/// The route of the child process, until it is killed. Does nothing when run
/// on its own.
#[tokio::test]
#[ignore = "run by crash::a_killed_route_loses_nothing_on_restart"]
async fn crash_child() -> anyhow::Result<()> {
    let Ok(settings) = std::env::var(CHILD_ENV) else {
        return Ok(());
    };
    let settings: Value = serde_json::from_str(&settings)?;
    let route = route(
        serde_json::from_value(settings["route"].clone())?,
        serde_json::from_value(settings["input"].clone())?,
        serde_json::from_value(settings["output"].clone())?,
    )?;
    let (_trigger, shutdown) = triggered::trigger();
    Box::new(route).start_task(shutdown).await?;
    Ok(())
}

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
        "route": { "state_dir": h.route_settings(ROUTE).state_dir },
        "input": input_json(&input),
        "output": output_json(&output)?,
    });

    // Created before the child opens it too (see the shutdown test).
    let ledger = h.ledger(ROUTE).await?;
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args(["crash::crash_child", "--exact", "--ignored"])
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
    common::run(route(
        h.route_settings(ROUTE),
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
