//! Fixtures shared by the integration tests: one record type fed by two
//! inputs with different protobuf messages, two outputs with different row
//! types, a table of the middleware's errors, and a middleware rejecting one
//! kind of record.

pub mod harness;
pub mod metrics;

pub use harness::Harness;

use chrono::{DateTime, Duration, Utc};
use glacon_helium::{
    BatchId, BoxError, FileMeta, FileStoreInput, GlaconCoreMiddleware, GlaconInput, GlaconOutput,
    GlaconRecordError, IcebergOutput, RecordResult, Route, RouteBuilder, RouteOutputs, async_trait,
    helium_iceberg::{FieldDefinition, PartitionDefinition, TableDefinition},
    task_manager::ManagedTask,
};
use glacon_test_proto::{EventV1, EventV2, event_v2};
use serde::Serialize;
use trino_rust_client::Trino;

pub const V1_PREFIX: &str = "event_v1";
pub const V2_PREFIX: &str = "event_v2";
pub const EVENTS: &str = "events";
pub const VALUES: &str = "values";
pub const REJECTS: &str = "rejects";

// ---------------------------------------------------------------- record

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    A,
    B,
    Both,
    Drop,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::A => "a",
            Kind::B => "b",
            Kind::Both => "both",
            Kind::Drop => "drop",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Event {
    pub id: String,
    pub occurred_at: DateTime<Utc>,
    pub kind: Kind,
    pub values: Vec<i64>,
    pub source: &'static str,
}

// ---------------------------------------------------------------- inputs

/// Decodes `EventV1`. A kind it does not know ("bad", ...) fails decoding.
pub struct V1Input;

impl GlaconInput for V1Input {
    type Proto = EventV1;
    type Record = Event;

    fn prefix(&self) -> &str {
        V1_PREFIX
    }

    fn decode(&self, proto: EventV1, _file: FileMeta<'_>) -> Option<Event> {
        let kind = match proto.kind.as_str() {
            "a" => Kind::A,
            "b" => Kind::B,
            "both" => Kind::Both,
            "drop" => Kind::Drop,
            _ => return None,
        };
        Some(Event {
            id: proto.id,
            occurred_at: DateTime::from_timestamp_millis(proto.timestamp_ms as i64)?,
            kind,
            values: vec![proto.value],
            source: "v1",
        })
    }
}

/// Decodes `EventV2`, whose shape differs from `EventV1`.
pub struct V2Input;

impl GlaconInput for V2Input {
    type Proto = EventV2;
    type Record = Event;

    fn prefix(&self) -> &str {
        V2_PREFIX
    }

    fn decode(&self, proto: EventV2, _file: FileMeta<'_>) -> Option<Event> {
        let kind = match proto.kind() {
            event_v2::Kind::A => Kind::A,
            event_v2::Kind::B => Kind::B,
            event_v2::Kind::Both => Kind::Both,
            event_v2::Kind::Drop => Kind::Drop,
            event_v2::Kind::Unspecified => return None,
        };
        Some(Event {
            id: proto.event_id,
            occurred_at: DateTime::from_timestamp_millis(proto.occurred_at_ms as i64)?,
            kind,
            values: proto.values,
            source: "v2",
        })
    }
}

pub fn v1(id: &str, kind: &str, value: i64) -> EventV1 {
    EventV1 {
        id: id.to_string(),
        timestamp_ms: Utc::now().timestamp_millis() as u64,
        kind: kind.to_string(),
        value,
    }
}

pub fn v2(id: &str, kind: event_v2::Kind, values: &[i64]) -> EventV2 {
    EventV2 {
        event_id: id.to_string(),
        occurred_at_ms: Utc::now().timestamp_millis() as u64,
        kind: kind as i32,
        values: values.to_vec(),
    }
}

// ---------------------------------------------------------------- outputs

/// One row per event.
pub struct EventsTable(pub &'static str);

#[derive(Serialize)]
pub struct EventRow {
    id: Option<String>,
    kind: &'static str,
    source: &'static str,
    value_sum: i64,
    occurred_at: DateTime<Utc>,
}

impl<E: GlaconRecordError> GlaconOutput<Event, E> for EventsTable {
    type Row = EventRow;

    fn table(&self) -> glacon_helium::helium_iceberg::Result<TableDefinition> {
        TableDefinition::builder("default", self.0)
            .with_fields([
                FieldDefinition::optional_string("id"),
                FieldDefinition::required_string("kind"),
                FieldDefinition::required_string("source"),
                FieldDefinition::required_long("value_sum"),
                FieldDefinition::required_timestamptz("occurred_at"),
            ])
            .with_partition(PartitionDefinition::day("occurred_at", "occurred_at_day"))
            .build()
    }

    fn to_rows(&self, event: &Event, rows: &mut Vec<EventRow>) {
        rows.push(EventRow {
            id: (!event.id.is_empty()).then(|| event.id.clone()),
            kind: event.kind.as_str(),
            source: event.source,
            value_sum: event.values.iter().sum(),
            occurred_at: event.occurred_at,
        });
    }
}

/// One row per value of an event: zero, one or many rows per record.
pub struct ValuesTable(pub &'static str);

#[derive(Serialize)]
pub struct ValueRow {
    event_id: String,
    position: i32,
    value: i64,
    occurred_at: DateTime<Utc>,
}

impl<E: GlaconRecordError> GlaconOutput<Event, E> for ValuesTable {
    type Row = ValueRow;

    fn table(&self) -> glacon_helium::helium_iceberg::Result<TableDefinition> {
        TableDefinition::builder("default", self.0)
            .with_fields([
                FieldDefinition::required_string("event_id"),
                FieldDefinition::required_int("position"),
                FieldDefinition::required_long("value"),
                FieldDefinition::required_timestamptz("occurred_at"),
            ])
            .with_partition(PartitionDefinition::day("occurred_at", "occurred_at_day"))
            .build()
    }

    fn to_rows(&self, event: &Event, rows: &mut Vec<ValueRow>) {
        rows.extend(event.values.iter().enumerate().map(|(i, value)| ValueRow {
            event_id: event.id.clone(),
            position: i as i32,
            value: *value,
            occurred_at: event.occurred_at,
        }));
    }
}

/// A table keeping only the events of some kinds: `Only(EventsTable("t"),
/// &[Kind::A, Kind::Both])`.
pub struct Only<T>(pub T, pub &'static [Kind]);

impl<E: GlaconRecordError, T: GlaconOutput<Event, E>> GlaconOutput<Event, E> for Only<T> {
    type Row = T::Row;

    fn table(&self) -> glacon_helium::helium_iceberg::Result<TableDefinition> {
        self.0.table()
    }

    fn to_rows(&self, event: &Event, rows: &mut Vec<T::Row>) {
        if self.1.contains(&event.kind) {
            self.0.to_rows(event, rows);
        }
    }
}

/// One row per error of the middleware; no row for its records.
pub struct RejectsTable(pub &'static str);

#[derive(Serialize)]
pub struct RejectRow {
    error: String,
    rejected_at: DateTime<Utc>,
}

impl GlaconOutput<Event, Rejected> for RejectsTable {
    type Row = RejectRow;

    fn table(&self) -> glacon_helium::helium_iceberg::Result<TableDefinition> {
        TableDefinition::builder("default", self.0)
            .with_fields([
                FieldDefinition::required_string("error"),
                FieldDefinition::required_timestamptz("rejected_at"),
            ])
            .with_partition(PartitionDefinition::day("rejected_at", "rejected_at_day"))
            .build()
    }

    fn to_rows(&self, _: &Event, _: &mut Vec<RejectRow>) {}

    fn to_error_rows(&self, error: &Rejected, rows: &mut Vec<RejectRow>) {
        rows.push(RejectRow {
            error: error.to_string(),
            rejected_at: Utc::now(),
        });
    }
}

// ---------------------------------------------------------------- middleware

/// Why [`RejectDrop`] rejected an event.
#[derive(Debug)]
pub struct Rejected {
    pub id: String,
}

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: rejected", self.id)
    }
}

impl std::error::Error for Rejected {}

impl GlaconRecordError for Rejected {
    fn reason(&self) -> &'static str {
        "rejected"
    }
}

/// Rejects "drop" events (outputs get an error for each); the rest pass
/// unchanged.
#[derive(Clone)]
pub struct RejectDrop;

#[async_trait]
impl GlaconCoreMiddleware<Event, Event> for RejectDrop {
    type Error = Rejected;

    async fn handle_data(
        &mut self,
        _: &BatchId,
        events: Vec<Event>,
    ) -> Result<Vec<RecordResult<Event, Rejected>>, BoxError> {
        Ok(events
            .into_iter()
            .map(|e| match e.kind {
                Kind::Drop => Err(Rejected { id: e.id }),
                _ => Ok(e),
            })
            .collect())
    }
}

// ---------------------------------------------------------------- running

/// Files are timestamped relative to now; inputs start one hour back.
pub fn start_after() -> DateTime<Utc> {
    Utc::now() - Duration::hours(1)
}

pub fn minutes_ago(minutes: i64) -> DateTime<Utc> {
    Utc::now() - Duration::minutes(minutes)
}

/// Route `name` reading bucket `b`.
pub fn input_builder(h: &Harness, name: &str, b: usize) -> RouteBuilder<Event> {
    Route::builder(name, h.route_settings(name)).input(
        "v1",
        FileStoreInput::new(V1Input, h.input_settings(b, start_after())),
    )
}

/// Route `name` from bucket `b` into the events table `table`.
pub fn route_builder(
    h: &Harness,
    name: &str,
    b: usize,
    table: &'static str,
) -> RouteOutputs<Event, Event> {
    input_builder(h, name, b).output(
        EVENTS,
        IcebergOutput::new(EventsTable(table), h.output_settings()),
    )
}

/// Run a route, or a TaskManager of routes, until it finishes (inputs stop
/// after their idle timeout).
pub async fn run(task: impl ManagedTask + 'static) -> anyhow::Result<()> {
    let (_trigger, shutdown) = triggered::trigger();
    tokio::time::timeout(
        humantime::parse_duration("2 minutes")?,
        Box::new(task).start_task(shutdown),
    )
    .await??;
    Ok(())
}

// ---------------------------------------------------------------- reading

#[derive(Debug, serde::Deserialize, serde::Serialize, Trino)]
struct Count {
    n: i64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Trino)]
pub struct EventOut {
    pub id: String,
    pub kind: String,
    pub source: String,
    pub value_sum: i64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Trino)]
pub struct ValueOut {
    pub event_id: String,
    pub position: i32,
    pub value: i64,
}

pub async fn count(harness: &Harness, table: &str) -> anyhow::Result<i64> {
    let rows = harness
        .iceberg
        .trino()
        .get_all::<Count>(format!("SELECT COUNT(*) AS n FROM \"default\".{table}"))
        .await?;
    Ok(rows.into_vec().first().map_or(0, |c| c.n))
}

/// Events ordered by id (an empty id sorts first).
pub async fn events(harness: &Harness, table: &str) -> anyhow::Result<Vec<EventOut>> {
    if count(harness, table).await? == 0 {
        return Ok(Vec::new());
    }
    Ok(harness
        .iceberg
        .trino()
        .get_all::<EventOut>(format!(
            "SELECT coalesce(id, '') AS id, kind, source, value_sum FROM \"default\".{table} \
             ORDER BY id, source, value_sum"
        ))
        .await?
        .into_vec())
}

pub async fn values(harness: &Harness, table: &str) -> anyhow::Result<Vec<ValueOut>> {
    if count(harness, table).await? == 0 {
        return Ok(Vec::new());
    }
    Ok(harness
        .iceberg
        .trino()
        .get_all::<ValueOut>(format!(
            "SELECT event_id, position, value FROM \"default\".{table} ORDER BY event_id, position"
        ))
        .await?
        .into_vec())
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Trino)]
struct RejectOut {
    error: String,
}

/// The errors of a rejects table, sorted.
pub async fn rejects(harness: &Harness, table: &str) -> anyhow::Result<Vec<String>> {
    if count(harness, table).await? == 0 {
        return Ok(Vec::new());
    }
    Ok(harness
        .iceberg
        .trino()
        .get_all::<RejectOut>(format!(
            "SELECT error FROM \"default\".{table} ORDER BY error"
        ))
        .await?
        .into_vec()
        .into_iter()
        .map(|r| r.error)
        .collect())
}

pub fn ids(events: &[EventOut]) -> Vec<&str> {
    events.iter().map(|e| e.id.as_str()).collect()
}
