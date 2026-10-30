# Runtime: tasks and the life of a record

How a running glacon app is split into Tokio tasks, and which task does each
step of moving data from an input, through the middleware, to an output.

The examples use one route, `events`, with one input `us` (a
`FileStoreInput`) and one output `lake` (an `IcebergOutput`).

## Tasks

A *task* is a Tokio task: a future started with `tokio::spawn` that the
runtime schedules on its worker threads. A route is an oracles
`ManagedTask`; once started, it runs one task per input, one processor per
input and one task per output, plus the background task each input or output
returned from `init` (glacon-helium's poller and writer).

```text
your task (e.g. main, calling task_manager.start().await)
└─ your TaskManager                    ← not spawned: runs inside your task
   └─ route task "events"              ← spawned (one per route)
      └─ route's own TaskManager       ← not spawned: runs inside the route task
         ├─ "output lake: background"  ← spawned: the writer IcebergOutput::init returned
         ├─ output task "lake"         ← spawned
         ├─ processor task "us"        ← spawned
         ├─ "input us: background"     ← spawned: the poller FileStoreInput::init returned
         └─ input task "us"            ← spawned
```

- **Your `TaskManager`** runs routes next to your other tasks. Routes are
  added with `add_task`; glacon has no runner of its own.
- **The route task** (`run` and `up`,
  [glacon-core/src/route/mod.rs](../glacon-core/src/route/mod.rs))
  initializes every input, middleware and output, then only drives the
  route's own `TaskManager`: it waits on the stage tasks, stops them in order,
  and fails if one of them fails. No data passes through it.
- **The input task** ([input/task.rs](../glacon-core/src/input/task.rs))
  calls your running input's `next_batch`, records each batch in the route's
  ledger (`state.db`), and sends it to its processor.
- **The processor task** ([processor/task.rs](../glacon-core/src/processor/task.rs))
  runs the input's own middleware over each batch and sends every output the
  transformed batch (on a replay, only the outputs that don't have it yet).
- **The output task** ([output/task.rs](../glacon-core/src/output/task.rs))
  calls your running output's `handle_data` with those records (it writes
  the ones it keeps), then records in the ledger that it has the batch.
- **Background tasks** are what an input's or output's `init` returned: for
  glacon-helium, the `file_store` poller and the `helium_iceberg` writer.
  The route starts and stops them like its own tasks, and fails if they do.

A route with N inputs and M outputs therefore runs 1 + 2N + M tasks, plus N
pollers and M writers with glacon-helium. The parent/child links are
`task_manager` bookkeeping: Tokio has no parent tasks, and all of them run as
peers on any worker thread.

Before any task starts, the route initializes outputs, then middlewares, then
inputs. Tasks are added output backgrounds, outputs, processors, input
backgrounds, then inputs, and stop in the reverse order: inputs stop reading,
then their pollers stop, processors handle what is queued, outputs write what
is queued and terminate, then their writers commit their spools and stop. A
background task always starts before its stage and stops after it.

## Where your code runs

| Your code | Task | When |
|---|---|---|
| `Clone` of your middleware | route | once per input, at start |
| `GlaconCoreOutputInit::init` (`IcebergOutput`: `GlaconOutput::table()`), returning the running output and its background task | route | once per output, at start |
| `GlaconCoreMiddleware::init` | route | once per input, at start |
| `GlaconCoreInputInit::init`, returning the running input and its background task | route | once per input, at start |
| `GlaconCoreInput::next_batch` (`FileStoreInput`: `GlaconInput::decode`) | input | per batch (per message for `decode`) |
| `GlaconCoreMiddleware::handle_data` | processor | per batch |
| `GlaconCoreOutput::already_written` | output | per replayed batch it doesn't have yet |
| `GlaconCoreOutput::handle_data` (`IcebergOutput`: `GlaconOutput::to_rows`) | output | per batch (per record for `to_rows`) |
| `serde::Serialize` of your `Row` | writer | when rows are converted to Arrow |
| `terminate` of middlewares and outputs | its own task | when it stops (the route task when undoing a failed start) |

`GlaconInput::decode`, `GlaconOutput::table` / `to_rows` and the middleware
factory are synchronous; the `GlaconCore*` hooks are async.

## The life of a record

Every step, in order, from startup to shutdown. "Yours" marks calls into your
code.

### 1. Up (once, in the route task)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 1 | Open the ledger, `<state_dir>/state.db` (create, WAL, migrate) | async (disk) | route | `route/mod.rs` `up`, `ledger/mod.rs` `open` |
| 2 | Init every output at once: connect the catalog, `GlaconOutput::table()` (yours), create namespace and table if missing, report whether the table already had a snapshot | async (HTTP) | route | `lifecycle.rs` `init_all`, `output/iceberg/output.rs` `init` |
| 3 | `BatchedWriter::new`, spooling to `<spool_dir>/events/lake`; return its task | sync | route | `IcebergOutput::init` |
| 4 | Fresh-state guard: a ledger never run on, with an output that has data, refuses to start (unless `FreshState::Proceed`); then mark the ledger initialized | async (disk) | route | `route/mod.rs` `guard_fresh_state` |
| 5 | Per input: run the middleware factory (yours), then every middleware's `init` (yours) at once | sync + async | route | `up` |
| 6 | Init every input at once: connect the bucket, create the poller, load its resume point from the ledger (the oldest `in_flight` file, else the newest done one) | async (S3 + disk) | route | `input/file_store/input.rs` `init`, `file_store/state.rs` |
| 7 | Return the poller as the input's background task | sync | route | `FileStoreInput::init` |
| 8 | Wire the channels; start the route's TaskManager: writer, output, processor, poller, input tasks | sync (spawn) | route | `route/mod.rs` `wire` |

If any `init` fails, or the guard refuses, the ones that succeeded in its
layer and every earlier layer are terminated, the background tasks they
returned are dropped, and the route fails without starting a task.

### 2. Writer startup (once, before the writer accepts rows)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 9 | Replay leftover spool files from a previous run and commit them | async (disk + S3 + HTTP) | writer `lake` | `helium_iceberg` `Spool::replay_dir` |
| 10 | Open a new spool file | async (disk) | writer `lake` | `Spool::create` |

### 3. Poller loop (repeats, one file per iteration)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 11 | Wait for a free slot in the poller → input channel (5 files; blocks here when the input is slow) | async (channel) | poller | `file_store` `FileInfoPollerServer::run` |
| 12 | List the bucket after `resume point − offset` | async (S3 `ListObjectsV2`) | poller | `get_next_file` |
| 13 | Skip files already sent (in-memory cache) and files the ledger has as done | async (disk) | poller | `is_already_processed`, `file_store/state.rs` `exists` |
| 14 | Nothing new: sleep `poll_duration`; close the channel after `idle_timeout` (if set) | async (timer) | poller | `get_next_file` |
| 15 | Download the file; gunzip, split frames, `prost` decode into `Proto` messages | async (S3 + stream) | poller | `get_raw`, `parse` |
| 16 | Send the file to the input | sync (slot reserved) | poller | `permit.send` |

### 4. Input (per file)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 17 | `next_batch`: receive the next file from the poller | async (channel) | input `us` | `input/task.rs` `read`, `FileStoreInput::next_batch` |
| 18 | `GlaconInput::decode` per message (yours); count decoded and failed messages | sync | input `us` | `file_store/input.rs` `decode` |
| 19 | Record the batch in the ledger: never seen → `in_flight`; already `in_flight` → a replay; done → skipped | async (disk) | input `us` | `input/task.rs` `read`, `ledger/mod.rs` `read` |
| 20 | Send the batch, with its replay flag, to the processor (blocks while one is already queued) | async (channel) | input → processor | `input/task.rs` `read` |

### 5. Processor (per batch)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 21 | Receive the batch (and hourly: forget expired done batches, report those in flight) | async (channel) | processor `us` | `processor/task.rs` `process_all`, `housekeep` |
| 22 | `GlaconCoreMiddleware::handle_data(&BatchId, records)` (yours): the transformed records, without the dropped ones | async | processor `us` | `process` |
| 23 | Count the records the middleware left out | sync | processor `us` | `process` |
| 24 | On a replay: leave out the outputs the ledger says already took the batch | async (disk) | processor `us` | `process` |
| 25 | No record or no output left: mark the batch done. Otherwise send every output the whole batch at once, with one shared ticket counting them (blocks while that output has one queued) | async (disk or channels) | processor → outputs | `process`, `output/delivery.rs` |
| 26 | Metrics + "processed batch" log | sync | processor `us` | `process` |

### 6. Output (per batch)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 27 | Receive the delivery: the batch, shared by every output | async (channel) | output `lake` | `output/task.rs` `write` |
| 28 | On a replay: `already_written(&BatchId)` (yours, optional, default no); yes skips steps 29 and 30 | async | output `lake` | `write` |
| 29 | `GlaconOutput::to_rows` per record (yours): no rows for the records this table doesn't keep | sync | output `lake` | `output/iceberg/output.rs` `handle_data` |
| 30 | `queue_all(rows)`: send to the writer, wait for its "on disk" ack (skipped when no rows) | async (channel + ack) | output → writer | `handle_data` |
| 31 | Record in the ledger that `lake` has the batch, then count down the ticket; the last output marks it done (deliveries cleared, one transaction) | async (disk) | output `lake` | `BatchTicket::delivered` |

### 7. Writer (per message, and per commit)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 32 | Receive the rows message | async (channel) | writer `lake` | `BatchedWriterTask::run` |
| 33 | Rows → Arrow batch (your `Row`'s `Serialize`, table schema) | sync | writer `lake` | `records_to_batch` |
| 34 | Append to the spool file | async (blocking pool) | writer `lake` | `Spool::append` |
| 35 | Ack the output (unblocks step 30) | sync | writer `lake` | `ack.send` |
| 36 | Commit when `max_batch_rows` is reached or `batch_timeout` fires: fsync the spool, write Parquet to S3, `fast_append` in the catalog, delete the spool file | async (disk + S3 + HTTP) | writer `lake` | `flush_to_iceberg` |

### 8. Down (reverse of start order)

| # | Step | Kind | Task | Code |
|---|---|---|---|---|
| 37 | Input stops reading; the input is dropped | async | input `us` | `input/task.rs` `run` |
| 38 | Poller stops | async | input `us`: background | `file_store` `FileInfoPollerServer::run` |
| 39 | Processor handles what is queued, waits for its input to be gone, then `terminate` of its middleware (yours) | async | processor `us` | `processor/task.rs` `run` |
| 40 | Output writes what is queued, then `terminate` (yours); the output is dropped with its writer handle | async | output `lake` | `output/task.rs` `run` |
| 41 | Writer: final commit of the spool, then exits | async (disk + S3 + HTTP) | output `lake`: background | `BatchedWriterTask::run` |
| 42 | Route's TaskManager finishes; route task returns | async | route | `route/mod.rs` `run` |

On shutdown, the route's TaskManager stops the tasks in this order. When the
input is done (the poller stopped after `idle_timeout`), the same order
follows from the channels: the poller closes its channel, the input returns,
its processor sees its channel close, the outputs see every processor gone,
and each writer sees its output's handle dropped.

### Reading the trace

- **Hand-offs between tasks** happen at steps 16 → 17 (the poller's file
  channel), 20 → 21 (input → processor), 25 → 27 (processor → each output)
  and 30 → 32 → 35 (output → writer, plus its ack). All are bounded, which is
  where backpressure comes from: a slow step anywhere makes everything before
  it wait, down to the poller at step 11, and files wait in S3 instead of in
  memory.
- **`state.db` (the ledger)** is the only record of progress: written at step
  19 (a batch is `in_flight`) and step 31 (an output has it, then it is
  done), read at steps 6 and 13 (where to resume, which files are done) and
  24 (which outputs a replay skips). It holds no records.
- **A file is done** once every output has it (step 31),
  that is once its rows are in each writer's spool (step 35). A crash before
  that reads it again on the next run, and delivers it only to the outputs
  that don't have it. The only duplicate glacon allows: a crash between steps
  30 and 31, for that output and batch, unless the output implements
  `already_written`. The writer adds its own: a crash during step 36, after
  the commit but before the spool file is deleted, commits that file again
  at step 9.
