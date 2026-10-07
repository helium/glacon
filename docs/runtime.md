# Runtime: tasks and the life of a record

How a running glacon app is split into Tokio tasks, and which task does each
step of moving data from an input, through the middleware, to an output.

The examples use one route, `events`, with one input `us` and one output
`lake`.

## Tasks

A *task* is a Tokio task: a future started with `tokio::spawn` that the
runtime schedules on its worker threads. Each route runs as one task, which
starts one task per input (poller), one processor and one task per output
(writer).

```text
your task (e.g. main, calling glacon.run().await)
└─ Glacon's TaskManager             ← not spawned: runs inside your task
   └─ route task "events"            ← spawned (one per route)
      └─ route's own TaskManager     ← not spawned: runs inside the route task
         ├─ writer task "lake"       ← spawned
         ├─ processor task           ← spawned
         └─ poller task "us"         ← spawned
```

- **`Glacon`** is not a task. Its `TaskManager` runs in whatever task awaits
  it, starts one task per route, and handles SIGTERM / Ctrl+C.
- **The route task** (`Route::run`, [src/route/mod.rs](../src/route/mod.rs))
  runs the route's setup, then only drives the route's own `TaskManager`: it
  waits on the children, stops them in order, and fails if one of them fails.
  No data passes through it.
- **The processor task** (`Processor::run`,
  [src/route/processor.rs](../src/route/processor.rs)) does all the per-record
  work, including every per-record call into your trait implementations.
- **Poller and writer tasks** are `file_store` and `helium_iceberg` tasks, one
  per input and one per output.

A route with N inputs and M outputs therefore runs N + M + 2 tasks. The
parent/child links are `task_manager` bookkeeping: Tokio has no parent tasks,
and all of them run as peers on any worker thread.

Tasks are started writers first, then the processor, then pollers, and stop in
the reverse order: pollers stop taking files, the processor finishes its
current file, then writers commit what is left in their spools.

## Where your code runs

| Your code | Task | When |
|---|---|---|
| `middleware_with(...)` factory | the caller of `build()` (usually your task) | once, before anything starts |
| `GlaconOutput::table()` | route | once per output, during setup |
| `GlaconInput::decode` | processor | per message |
| `GlaconRecord::dedup_key` | processor | per record |
| `GlaconMiddleware::route` | processor | per record |
| `GlaconOutput::to_rows` | processor | per record, per selected output |
| `serde::Serialize` of your `Row` | writer | when rows are converted to Arrow |

All of these are synchronous. Everything the library awaits (S3, SQLite,
Iceberg, channels) happens between them.

## The life of a record

Every step, in order, from startup to shutdown. "Yours" marks calls into your
trait implementations.

### 1. Setup (once, in `Route::setup`)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 1 | Open `state.db`: create dir, connect, run migrations | async (fs + SQLite) | route | `state.rs` `StateDb::open` |
| 2 | Check whether the DB was ever initialized | async (SQLite) | route | `StateDb::is_fresh` |
| 3 | Connect the Iceberg catalog | async (HTTP) | route | `output/runtime/entry.rs` `connect` |
| 4 | `GlaconOutput::table()` (yours) | sync | route | `connect` |
| 5 | Create namespace; check whether the table exists and has a snapshot | async (HTTP) | route | `connect` |
| 6 | Create the table if missing | async (HTTP) | route | `connect` |
| 7 | `BatchedWriter::new`: writer channel + writer task struct | sync | route | `connect` |
| 8 | Fresh-state guard: refuse or proceed | sync | route | `route/mod.rs` `setup` |
| 9 | Mark the DB initialized | async (SQLite) | route | `StateDb::mark_initialized` |
| 10 | Connect the S3 bucket client | async | route | `input/runtime/entry.rs` `start` |
| 11 | Create the poller; load its resume point (`MAX(file_timestamp_ms)`) | async (SQLite) | route | `start` + `poller_state.rs` |
| 12 | Wrap the poller channel as a stream with `.then(decode)`; merge inputs (`select_all`) | sync | route | `start`, `setup` |
| 13 | Run the middleware factory given to `middleware_with` (yours) | sync | caller of `build()` | `route/builder.rs` |
| 14 | Start the route's TaskManager: spawn writer, processor, poller | sync (spawn) | route | `tasks/mod.rs` |

### 2. Writer startup (once, before the writer accepts rows)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 15 | Replay leftover spool files from a previous run and commit them | async (disk + S3 + HTTP) | writer `lake` | `helium_iceberg` `Spool::replay_dir` |
| 16 | Open a new spool file | async (disk) | writer `lake` | `Spool::create` |

### 3. Poller loop (repeats, one file per iteration)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 17 | Wait for a free slot in the poller → processor channel (blocks here when the processor is slow) | async (channel) | poller `us` | `file_store` `FileInfoPollerServer::run` |
| 18 | List the bucket after `latest − offset` | async (S3 `ListObjectsV2`) | poller `us` | `get_next_file` |
| 19 | Skip files already sent (in-memory cache) | sync | poller `us` | `is_already_processed` |
| 20 | Skip files already processed (`exists` → `processed_files`) | async (SQLite) | poller `us` | `poller_state.rs` `exists` |
| 21 | Nothing new: sleep `poll_duration`; stop after `idle_timeout` | async (timer) | poller `us` | `get_next_file` |
| 22 | Download the file | async (S3 `GetObject`) | poller `us` | `get_raw` |
| 23 | Gunzip, split frames, `prost` decode into `Proto` messages | async (stream) | poller `us` | `ProstFileInfoPollerParser::parse` |
| 24 | Send the file (`FileInfoStream`) to the processor | sync (slot reserved) | poller `us` | `permit.send` |
| 25 | About every 12h: delete old `processed_files` rows | async (SQLite) | poller `us` | `poller_state.rs` `clean` |

### 4. Processor (per file)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 26 | Next file from the merged stream | async (channel) | processor | `route/processor.rs` `run` |
| 27 | Unwrap the messages (`NoopRecorder` records nothing) | async | processor | `input/runtime/entry.rs` `decode` |
| 28 | `GlaconInput::decode` per message (yours) | sync | processor | `decode` |
| 29 | Count decoded and failed messages; one error log per file with failures | sync | processor | `decode` |
| 30 | `GlaconRecord::dedup_key` per record (yours), hash it, drop in-file repeats | sync | processor | `Processor::dedup` |
| 31 | Look up known keys | async (SQLite) | processor | `StateDb::known_keys` |
| 32 | Drop records whose key is known | sync | processor | `dedup` |
| 33 | `GlaconMiddleware::route` per record (yours) | sync | processor | `process` |
| 34 | `GlaconOutput::to_rows` per selected record (yours), into the sink's buffer | sync | processor | `batched_sink.rs` `stage` |
| 35 | Read `in_flight_deliveries` (crash-recovery ledger) | async (SQLite) | processor | `StateDb::in_flight_deliveries` |
| 36 | `queue_all(rows)`: send to the writer, wait for its "on disk" ack | async (channel + ack) | processor → writer | `batched_sink.rs` `flush` |
| 37 | Record that `lake` took this file | async (SQLite) | processor | `record_in_flight_delivery` |
| 38 | Mark the file processed, upsert dedup keys, clear in-flight rows (one transaction) | async (SQLite tx) | processor | `StateDb::complete_file` |
| 39 | Prune old dedup keys (at most hourly) | async (SQLite) | processor | `prune_dedup_keys_if_due` |
| 40 | Metrics + "processed file" log | sync | processor | `process` |

### 5. Writer (per message, and per commit)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 41 | Receive the rows message | async (channel) | writer `lake` | `BatchedWriterTask::run` |
| 42 | Rows → Arrow batch (your `Row`'s `Serialize`, table schema) | sync | writer `lake` | `records_to_batch` |
| 43 | Append to the spool file | async (blocking pool) | writer `lake` | `Spool::append` |
| 44 | Ack the processor (unblocks step 36) | sync | writer `lake` | `ack.send` |
| 45 | Commit when `max_batch_rows` is reached or `batch_timeout` fires: fsync the spool, write Parquet to S3, `fast_append` in the catalog, delete the spool file | async (disk + S3 + HTTP) | writer `lake` | `flush_to_iceberg` |

### 6. Shutdown (reverse of start order)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 46 | Poller stops (shutdown signal, or idle timeout); its channel closes | async | poller `us` | `run` |
| 47 | Processor finishes its current file; the stream ends; dropping the processor drops the writer handles | async | processor | `run` |
| 48 | Writer sees its channel close: final commit of the spool, then exits | async (disk + S3 + HTTP) | writer `lake` | `run` (`channel_closed`) |
| 49 | Route's TaskManager finishes; route task returns | async | route | `Route::run` |

### Reading the trace

- **Hand-offs between tasks** happen only at steps 17 → 26 (the file channel)
  and 36 → 41 → 44 (the rows channel plus its ack). Both are bounded, which is
  where backpressure comes from: a slow step anywhere in the processor or a
  slow spool write makes the pollers wait at step 17, and files wait in S3
  instead of in memory.
- **`state.db`** is used by the route task during setup, by the poller
  (resume point, `exists`, cleanup) and by the processor (dedup, ledger,
  completion). Writers never touch it.
- **A file counts as processed** (step 38) only after every output has its
  rows durably in its spool (step 36). A crash before that means the file is
  read again, and the ledger (steps 35 and 37) keeps an output that already
  took it from getting its rows twice.
