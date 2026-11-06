# glacon-helium

Route protobuf files from S3 buckets into Iceberg tables, on
[glacon-core](../glacon-core/README.md): read that first for routes, the
middleware and the delivery guarantees.

This crate implements one input and one output, each wrapping a small trait
you implement:

| You implement | Wrapped by | Does |
|---|---|---|
| `GlaconInput` | `FileStoreInput::new(input, FileStoreInputSettings)` | One kind of file: file prefix + protobuf message → record. Returning `None` drops the message (logged at error, counted). |
| `GlaconOutput` | `IcebergOutput::new(output, IcebergOutputSettings)` | One table: its definition + record → 0..n rows. It gets every record: no rows drops one. |

Everything from glacon-core is re-exported, as are `file_store` and
`helium_iceberg`, so apps depend on this crate alone.
`FileStoreInputSettings` and `IcebergOutputSettings` are serde-ready so apps
can embed them in their own configuration.

## Usage

The middleware of [glacon-core's example](../glacon-core/README.md#usage)
decides once whether a speed test is valid. Here each bucket is a
`FileStoreInput`, and each table an `IcebergOutput` keeping what it wants:
valid tests in one, invalid ones in the other.

```rust
use glacon_helium::{
    FileMeta, FileStoreInput, GlaconInput, GlaconOutput, IcebergOutput, Route, RouteSettings,
    helium_iceberg::{self, TableDefinition},
    task_manager::TaskManager,
};

struct SpeedTestInput;

impl GlaconInput for SpeedTestInput {
    type Proto = SpeedTestProto;
    type Record = SpeedTest;

    fn prefix(&self) -> &str {
        "speedtest"
    }

    fn decode(&self, proto: SpeedTestProto, _: FileMeta<'_>) -> Option<SpeedTest> {
        SpeedTest::try_from(proto).ok() // None: logged and counted, then dropped
    }
}

struct VerifiedTable;

impl GlaconOutput<VerifiedSpeedTest> for VerifiedTable {
    type Row = VerifiedRow;

    fn table(&self) -> helium_iceberg::Result<TableDefinition> {
        // namespace, name, schema, partitioning
    }

    fn to_rows(&self, t: &VerifiedSpeedTest, rows: &mut Vec<VerifiedRow>) {
        if t.invalid_reason.is_none() {
            rows.push(VerifiedRow::from(&t.test));
        }
    }
}
// DroppedTable keeps the tests whose invalid_reason is set, with the reason.

let route = Route::builder("speedtests", RouteSettings::new("/data/speedtests"))
    .input("us", FileStoreInput::new(SpeedTestInput, us_settings))
    .input("eu", FileStoreInput::new(SpeedTestInput, eu_settings))
    .middleware(VerifySpeedTest)
    .output("verified", IcebergOutput::new(VerifiedTable, verified_settings))
    .output("dropped", IcebergOutput::new(DroppedTable, dropped_settings))
    .build()?;

TaskManager::builder().add_task(route).build().start().await?;
```

## `FileStoreInput`

Polls one bucket with `file_store`. Each file is one batch, keyed by its S3
key; its messages are decoded by your `GlaconInput`.

- Its `init` builds the poller (named after the input) and returns it as its
  background task; `next_batch` decodes the next file the poller found.
- When the poller stops on its own (its `idle_timeout`), the input is done;
  if it fails, the route does.
- The poller's state is the route's ledger: it resumes at the oldest file
  not delivered everywhere (minus its `offset`, to catch late uploads), and
  skips the files that were. Files stay in their bucket, so a file is read
  again after a crash until it is done.

`FileStoreInputSettings`:

| Field | Default | |
|---|---|---|
| `bucket` | — | `file_store::BucketSettings` |
| `start_after` | — | Files at or before this time are ignored; the starting point on the first run |
| `poll_duration` | 30s (`file_store`) | How often the bucket is listed |
| `offset` | 10 minutes (`file_store`) | How far before its resume point the poller lists again. Keep it within the route's `processed_retention`: a done file forgotten, then listed again, is delivered again |
| `idle_timeout` | never | Stop polling after this long without new files; the route ends once all its inputs have |

## `IcebergOutput`

Writes one table with `helium_iceberg`'s `BatchedWriter`; your
`GlaconOutput` defines the table and makes its rows.

- Its `init` creates the namespace and table if missing, and returns the
  writer's task as its background task, spooling to
  `<spool_dir>/<route>/<output>`.
- `handle_data` returns once the rows are in the spool: that is the
  acknowledgement. A batch it keeps no row of writes nothing.
- Rows are committed every `max_batch_rows` rows or `batch_timeout`, and
  when the route stops the writer.
- Its `init` reports existing data when the table already has a snapshot,
  which arms glacon-core's fresh-state guard.
- Tables must be partitioned: `helium_iceberg` cannot write to an
  unpartitioned table. Schema changes are not handled.

`IcebergOutputSettings`:

| Field | Default | |
|---|---|---|
| `catalog` | — | `helium_iceberg::Settings` |
| `spool_dir` | — | Durable directory for rows written but not committed yet; outputs can share it |
| `max_batch_rows` | 10 000 | Rows buffered before a commit |
| `batch_timeout` | 15 minutes | Longest time rows wait before a commit |

### Durability

`IcebergOutput` acknowledges a batch once its rows are in the spool, which
survives a process crash: the writer commits it on its next start. Two
caveats, from `helium_iceberg`:

- the spool is only fsynced at commit, so a machine or power loss can drop
  rows the ledger counts as delivered;
- a crash after a commit but before its spool file is removed commits that
  file again on the next start, duplicating up to `max_batch_rows` rows.

## Metrics

On top of glacon-core's:

- `glacon_records_decoded_total` (`FileStoreInput`)
- `glacon_decode_failures_total` (`FileStoreInput`)
- `glacon_output_rows_total` (`IcebergOutput`): rows kept, where
  `glacon_output_records_total` counts the records handed to the output

## Tests

The integration tests need RustFS, Polaris and Trino (`docker-compose.yml`
at the repository root; every config is inlined there) and `protoc` for the
test messages:

```bash
docker compose up -d --wait
cargo nextest run -p glacon-helium
```

Use `cargo nextest run`, not `cargo test`: under `cargo test`, every test gets
its own Tokio runtime inside one process, and `file_store`'s cached S3
clients then fail across runtimes. `.config/nextest.toml` also caps how many
of these tests run at once against the shared Polaris.

`tests/integration` has one file per implementation (`file_store_input.rs`,
`iceberg_output.rs`) and one for a middleware between them
(`middleware.rs`). The harness (`tests/integration/common/harness.rs`) gives
each test its own Iceberg catalog, buckets, spool and state directories.
`a_killed_route_loses_nothing_on_restart` kills a route with SIGKILL mid-run
and checks that a restart loses nothing.
