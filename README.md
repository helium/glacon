# glacon

A library to route protobuf files from S3 buckets into Iceberg tables.

A **route** reads one record type from **N inputs** (buckets), lets one
**middleware** pick which of its **M outputs** (tables) get each record, and
keeps its progress in its own SQLite database. `Glacon` runs every route as a
task on oracles' `task_manager`.

```text
input "us" ─┐                          ┌─► output "lake"    (rows A)
            ├─► record ─► middleware ──┤
input "eu" ─┘                          └─► output "archive" (rows B)
```

Built on [helium/oracles](https://github.com/helium/oracles): `file_store`
polls the buckets, `helium_iceberg` writes the tables.

## What you implement

| Trait | Does |
|---|---|
| `GlaconRecord` | The route's record type. Optional `dedup_key()`; `None` means never deduplicated. |
| `GlaconInput` | One kind of file: file prefix + protobuf message → record. Returning `None` drops the message (logged at error, counted). |
| `GlaconMiddleware` | Record → `Destination::All` (default), `Only(outputs)` or `Drop`. Created by `middleware_with`, which looks its outputs up by name (an unknown name fails `build()`). |
| `GlaconOutput` | One table: its definition + record → 0..n rows. |

```rust
let route = Route::builder("events", RouteSettings::new("/var/lib/glacon/events"))
    .input("us", EventV1Input, us_input_settings)
    .input("eu", EventV2Input, eu_input_settings)
    .output("lake", EventsTable, lake_settings)
    .output("values", ValuesTable, values_settings)
    // Created during build(), with the ids of the outputs it selects.
    .middleware_with(|outputs| {
        Ok(MyMiddleware {
            lake: outputs.id("lake")?,
            values: outputs.id("values")?,
        })
    })
    .build()?;

Glacon::new().route(route).run().await?;
```

`RouteSettings`, `InputSettings` and `OutputSettings` are serde-ready so apps
can embed them in their own configuration.

## How a route runs

Each route is one task with its own task manager: N pollers feed one
processor, which feeds M writers. Here with inputs `us`/`eu` and outputs
`lake`/`archive`, labelled with the files that implement each part:

```mermaid
flowchart TB
    classDef glacon fill:#EEEDFE,stroke:#534AB7,color:#3C3489
    classDef oracles fill:#F1EFE8,stroke:#5F5E5A,color:#444441
    classDef state fill:#E1F5EE,stroke:#0F6E56,color:#085041

    bus[("S3 bucket us")]:::oracles
    beu[("S3 bucket eu")]:::oracles
    pus["Poller us (file_store)<br/>input/runtime/poller_state.rs"]:::oracles
    peu["Poller eu (file_store)<br/>input/runtime/poller_state.rs"]:::oracles
    dus["decode() with Input V1<br/>input/runtime/entry.rs"]:::glacon
    deu["decode() with Input V2<br/>input/runtime/entry.rs"]:::glacon
    merge["Merged stream (select_all)<br/>route/mod.rs"]:::glacon
    proc["Processor, one file at a time<br/>route/processor.rs"]:::glacon
    db[("state.db<br/>state.rs")]:::state
    slake["BatchedSink lake<br/>output/runtime/batched_sink.rs"]:::glacon
    sarch["BatchedSink archive<br/>output/runtime/batched_sink.rs"]:::glacon
    wlake["BatchedWriterTask lake<br/>helium_iceberg, spool/lake/"]:::oracles
    warch["BatchedWriterTask archive<br/>helium_iceberg, spool/archive/"]:::oracles
    tlake[("Iceberg table lake")]:::oracles
    tarch[("Iceberg table archive")]:::oracles

    bus --> pus --> dus --> merge
    beu --> peu --> deu --> merge
    merge --> proc
    proc --> slake --> wlake -- "commit on size / timeout" --> tlake
    proc --> sarch --> warch -- "commit on size / timeout" --> tarch
    proc <--> db
    pus -. "processed_files" .-> db
    peu -. "processed_files" .-> db
```

Purple is glacon code, gray is oracles crates and storage, green is the
route's state.

What the processor does with one file, and which `state.db` table each step
touches:

```mermaid
flowchart TB
    classDef glacon fill:#EEEDFE,stroke:#534AB7,color:#3C3489
    classDef state fill:#E1F5EE,stroke:#0F6E56,color:#085041

    file["DecodedFile from us or eu<br/>input/runtime/decoded_file.rs"]:::glacon
    dedup["dedup(): repeats in the file, then known keys<br/>route/processor.rs, GlaconRecord::dedup_key"]:::glacon
    mw["GlaconMiddleware::route() per record<br/>middleware/, Destination, OutputSet"]:::glacon
    stage["stage() rows into lake / archive<br/>output/runtime/batched_sink.rs, GlaconOutput::to_rows"]:::glacon
    flush["flush(): rows durable in each spool<br/>BatchedWriter::queue_all"]:::glacon
    complete["complete_file(): one transaction<br/>state.rs"]:::glacon
    prune["Prune dedup keys, emit metrics<br/>state.rs, metrics.rs"]:::glacon

    keys[("dedup_keys")]:::state
    inflight[("in_flight_deliveries")]:::state
    processed[("processed_files")]:::state

    file --> dedup --> mw --> stage --> flush --> complete --> prune
    dedup -. "known_keys()" .-> keys
    flush -. "skip outputs already done,<br/>record new ones" .-> inflight
    complete -. "insert file" .-> processed
    complete -. "upsert keys" .-> keys
    complete -. "clear file's rows" .-> inflight
```

A file lands in `processed_files` only after every output has its rows durably
in the spool. A crash before that means the file is read again, and
`in_flight_deliveries` keeps an output that already took it from getting its
rows twice.

- Each input file is read once. The processor handles one file at a time, in
  this order: decode, dedup, middleware, then each output's rows are spooled.
  A slow output stalls its route.
- Every output has a `helium_iceberg::BatchedWriter`: rows are spooled to disk,
  then committed in batches (`max_batch_rows` / `batch_timeout`).
- If any route fails, the whole process stops.

[docs/runtime.md](docs/runtime.md) shows how a route is split into Tokio
tasks, where your trait implementations run, and every step a record goes
through from setup to shutdown.

## State

`<state_dir>/state.db` (SQLite) is the route's only source of truth; nothing
is read back from the tables. It holds:

- `processed_files`: what each input has fully processed. It backs
  `file_store`'s poller state (resume point and skipping re-listed files) and
  is kept for `files_retention` (default 3 days).
- `in_flight_deliveries`: outputs that already took a file still in progress, so a
  re-read after a crash skips them.
- `dedup_keys`: dedup key hashes shared by all inputs, kept for
  `dedup_retention` (default 3 days).

`state_dir` must be durable. If a route starts with an empty state but an
output table already has data (a lost volume, for example), it refuses to start
unless built with `.fresh_state(FreshState::Proceed)`. Proceeding re-ingests
from each input's `start_after`.

Guarantee: at-least-once with a small duplicate window, narrowed by dedup.

## Constraints

- Output tables must be partitioned: `helium_iceberg` cannot write to an
  unpartitioned table.
- Missing tables are created from `GlaconOutput::table()`. Schema changes are not
  handled.
- At most 64 outputs per route.

## Metrics

Emitted through the `metrics` facade (the app installs an exporter). Every
series has a `route` label, plus `input` or `output` where it applies:

- `glacon_processed_files_total`
- `glacon_file_lag_seconds`
- `glacon_file_processing_seconds`
- `glacon_records_decoded_total`
- `glacon_decode_failures_total`
- `glacon_dedup_dropped_total`
- `glacon_middleware_dropped_total`
- `glacon_output_records_total`
- `glacon_output_rows_total`
- `glacon_state_pruned_total` (`kind` is `processed_files` or `dedup_keys`)

## Development

The integration tests need RustFS, Polaris and Trino (`docker-compose.yml`;
every config is inlined there) and `protoc` for the test messages:

```bash
docker compose up -d --wait
cargo nextest run
```

Use `cargo nextest run`, not `cargo test`: under `cargo test`, every test gets
its own Tokio runtime inside one process, and `file_store`'s cached S3 clients
then fail across runtimes. `.config/nextest.toml` also caps how many
integration tests run at once against the shared Polaris.

The test environment (`tests/integration/common/harness.rs`) gives each test
its own Iceberg catalog, buckets and state dirs.
