//! In-memory inputs, outputs, middleware and background tasks. A
//! [`Journal`] shared by everything in a test records every hook call, in
//! order, so tests can assert on the Up and Down order.
//!
//! Stages are fakes, but each route's ledger is a real `state.db` under a
//! temp dir the test owns: a test restarts a route by building it again on
//! the same dir.

use chrono::Utc;
use glacon_core::{
    Batch, BatchId, BoxError, Error, GlaconCoreInput, GlaconCoreMiddleware, GlaconCoreOutput,
    GlaconRecordError, InitializedInput, InitializedOutput, InputContext, Ledger,
    MiddlewareContext, OutputContext, RecordResult, Route, RouteBuilder, RouteOutputs,
    RouteSettings, async_trait,
    task_manager::{self, ManagedTask, TaskError, TaskFuture, TaskResult},
};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;

pub const LEFT: &str = "left";
pub const RIGHT: &str = "right";

// ---------------------------------------------------------------- record

#[derive(Debug, Clone)]
pub struct Event {
    pub id: String,
    /// "left", "right" or "both" (which [`MemoryOutput`]s keep it), "reject"
    /// or "fail" (see [`Checked`]).
    pub kind: &'static str,
}

pub fn event(id: &str, kind: &'static str) -> Event {
    Event {
        id: id.to_string(),
        kind,
    }
}

pub fn batch(key: &str, events: Vec<Event>) -> Batch<Event> {
    Batch {
        key: key.to_string(),
        timestamp: Utc::now(),
        records: events,
    }
}

// ---------------------------------------------------------------- journal

#[derive(Clone, Default)]
pub struct Journal(Arc<Mutex<Vec<String>>>);

impl Journal {
    pub fn push(&self, entry: impl Into<String>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(entry.into());
    }

    pub fn entries(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Positions of the entries starting with `prefix`.
    pub fn positions(&self, prefix: &str) -> Vec<usize> {
        self.entries()
            .iter()
            .enumerate()
            .filter(|(_, e)| e.starts_with(prefix))
            .map(|(i, _)| i)
            .collect()
    }

    pub fn contains(&self, entry: &str) -> bool {
        self.entries().iter().any(|e| e == entry)
    }

    /// Every entry starting with `before` comes before every entry starting
    /// with `after`, and there is at least one of each.
    pub fn assert_before(&self, before: &str, after: &str) {
        let (b, a) = (self.positions(before), self.positions(after));
        let entries = self.entries();
        assert!(
            !b.is_empty() && !a.is_empty(),
            "{before:?} / {after:?} missing: {entries:?}"
        );
        assert!(
            b.iter().max() < a.iter().min(),
            "{before:?} not all before {after:?}: {entries:?}"
        );
    }
}

// ---------------------------------------------------------------- background

/// A background task an input or output returns from `init`: journals
/// `start <name>` and `stop <name>`, and runs until stopped, fails or panics.
#[derive(Clone)]
pub struct Background {
    pub name: String,
    pub end: BackgroundEnd,
    pub journal: Journal,
}

#[derive(Clone, Copy)]
pub enum BackgroundEnd {
    /// Runs until the route stops it.
    UntilStopped,
    Fail,
    Panic,
}

impl ManagedTask for Background {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        let Background { name, end, journal } = *self;
        task_manager::spawn(async move {
            journal.push(format!("start {name}"));
            match end {
                BackgroundEnd::UntilStopped => shutdown.await,
                BackgroundEnd::Fail => return Err(BoxError::from("background failure")),
                BackgroundEnd::Panic => panic!("background panic"),
            }
            journal.push(format!("stop {name}"));
            Ok(())
        })
    }
}

// ---------------------------------------------------------------- input

/// Hands out its batches, then returns, fails, panics or waits to be
/// stopped. Its state journals `drop input <name>` when dropped: the marker
/// that its task ended.
pub struct MemoryInput {
    pub name: &'static str,
    pub batches: VecDeque<Batch<Event>>,
    pub end: End,
    pub fail_init: bool,
    /// Returned from `init` for the route to supervise.
    pub background: Option<BackgroundEnd>,
    pub journal: Journal,
}

#[derive(Clone, Copy)]
pub enum End {
    Return,
    /// Pending until the route stops the input.
    HoldOpen,
    Fail,
    Panic,
}

impl MemoryInput {
    pub fn new(
        name: &'static str,
        batches: Vec<Batch<Event>>,
        end: End,
        journal: &Journal,
    ) -> Self {
        Self {
            name,
            batches: batches.into(),
            end,
            fail_init: false,
            background: None,
            journal: journal.clone(),
        }
    }
}

#[async_trait]
impl GlaconCoreInput<Event> for MemoryInput {
    type State = MemoryState;

    async fn init(
        &mut self,
        ctx: &InputContext,
    ) -> Result<InitializedInput<MemoryState>, BoxError> {
        let name = self.name;
        assert_eq!(ctx.input(), name);
        if self.fail_init {
            return Err("input init failure".into());
        }
        self.journal.push(format!("init input {name}"));
        let task = self.background.take().map(|end| {
            Box::new(Background {
                name: format!("input {name}"),
                end,
                journal: self.journal.clone(),
            }) as Box<dyn ManagedTask>
        });
        Ok(InitializedInput {
            state: MemoryState {
                name,
                batches: std::mem::take(&mut self.batches),
                journal: self.journal.clone(),
            },
            task,
        })
    }

    async fn next_batch(
        &mut self,
        state: &mut MemoryState,
    ) -> Result<Option<Batch<Event>>, BoxError> {
        if let Some(batch) = state.batches.pop_front() {
            return Ok(Some(batch));
        }
        match self.end {
            End::Return => Ok(None),
            End::HoldOpen => std::future::pending().await,
            End::Fail => Err("input failure".into()),
            End::Panic => panic!("input panic"),
        }
    }
}

/// The state of a [`MemoryInput`]: the batches left.
pub struct MemoryState {
    name: &'static str,
    batches: VecDeque<Batch<Event>>,
    journal: Journal,
}

impl Drop for MemoryState {
    fn drop(&mut self) {
        self.journal.push(format!("drop input {}", self.name));
    }
}

/// An input shaped like a poller: its background task produces the batches
/// into a channel, then ends; the input reads the channel from its state, so it
/// ends once the background task has.
pub struct ChannelInput {
    pub name: &'static str,
    pub batches: Vec<Batch<Event>>,
    pub journal: Journal,
}

impl ChannelInput {
    pub fn new(name: &'static str, batches: Vec<Batch<Event>>, journal: &Journal) -> Self {
        Self {
            name,
            batches,
            journal: journal.clone(),
        }
    }
}

/// The background task of a [`ChannelInput`].
struct Feeder {
    name: String,
    batches: Vec<Batch<Event>>,
    sender: mpsc::Sender<Batch<Event>>,
    journal: Journal,
}

impl ManagedTask for Feeder {
    fn start_task(self: Box<Self>, _shutdown: triggered::Listener) -> TaskFuture {
        let Feeder {
            name,
            batches,
            sender,
            journal,
        } = *self;
        task_manager::spawn(async move {
            journal.push(format!("start {name}"));
            for batch in batches {
                if sender.send(batch).await.is_err() {
                    break;
                }
            }
            journal.push(format!("stop {name}"));
            Ok::<_, BoxError>(())
        })
    }
}

#[async_trait]
impl GlaconCoreInput<Event> for ChannelInput {
    /// What the feeder sends.
    type State = mpsc::Receiver<Batch<Event>>;

    async fn init(&mut self, _: &InputContext) -> Result<InitializedInput<Self::State>, BoxError> {
        let (sender, received) = mpsc::channel(1);
        let feeder = Feeder {
            name: format!("input {}", self.name),
            batches: std::mem::take(&mut self.batches),
            sender,
            journal: self.journal.clone(),
        };
        Ok(InitializedInput::new(received).with_task(feeder))
    }

    async fn next_batch(
        &mut self,
        received: &mut Self::State,
    ) -> Result<Option<Batch<Event>>, BoxError> {
        Ok(received.recv().await)
    }
}

// ---------------------------------------------------------------- output

/// Keeps the ids of the events whose kind is its name or "both", the
/// middleware's errors, and the batches it was handed. Clones share them, so a test keeps one to read
/// what the route wrote, and a restarted route writes to the same rows. It
/// reports existing data from `init` when it already has rows.
#[derive(Clone)]
pub struct MemoryOutput {
    pub name: &'static str,
    rows: Arc<Mutex<Vec<String>>>,
    /// The middleware's errors it was handed.
    errors: Arc<Mutex<Vec<String>>>,
    /// Keys of the batches handed to it, in order.
    batches: Arc<Mutex<Vec<String>>>,
    /// Writes left to fail before writing.
    failing_writes: Arc<AtomicUsize>,
    /// Writes left to fail after writing: a crash right after a durable
    /// write.
    failing_after_writes: Arc<AtomicUsize>,
    /// Answers `already_written` from the batches it wrote.
    pub knows_written: bool,
    pub fail_init: bool,
    /// Returned from `init` for the route to supervise.
    pub background: Option<BackgroundEnd>,
    journal: Journal,
}

impl MemoryOutput {
    pub fn new(name: &'static str, journal: &Journal) -> Self {
        Self {
            name,
            rows: Default::default(),
            errors: Default::default(),
            batches: Default::default(),
            failing_writes: Default::default(),
            failing_after_writes: Default::default(),
            knows_written: false,
            fail_init: false,
            background: None,
            journal: journal.clone(),
        }
    }

    /// Fail the next `n` writes.
    pub fn fail_writes(&self, n: usize) {
        self.failing_writes.store(n, Ordering::SeqCst);
    }

    /// Write the next `n` batches, then fail.
    pub fn fail_after_writes(&self, n: usize) {
        self.failing_after_writes.store(n, Ordering::SeqCst);
    }

    /// Keys of the batches handed to it, in order (a key twice: written
    /// twice), even those it kept nothing of.
    pub fn batches(&self) -> Vec<String> {
        self.batches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Ids written, sorted.
    pub fn rows(&self) -> Vec<String> {
        let mut rows = self
            .rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        rows.sort();
        rows
    }

    /// The middleware's errors it was handed, sorted.
    pub fn errors(&self) -> Vec<String> {
        let mut errors = self
            .errors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        errors.sort();
        errors
    }

    /// Resolves once at least `n` rows were written.
    pub async fn wait_for_rows(&self, n: usize) {
        while self
            .rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
            < n
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Takes one from `n` if not zero yet.
fn take_one(n: &AtomicUsize) -> bool {
    n.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
}

#[async_trait]
impl<E: GlaconRecordError> GlaconCoreOutput<Event, E> for MemoryOutput {
    type State = ();

    async fn init(&mut self, ctx: &OutputContext) -> Result<InitializedOutput<()>, BoxError> {
        assert_eq!(ctx.output(), self.name);
        if self.fail_init {
            return Err("output init failure".into());
        }
        self.journal.push(format!("init output {}", self.name));
        let task = self.background.take().map(|end| {
            Box::new(Background {
                name: format!("output {}", self.name),
                end,
                journal: self.journal.clone(),
            }) as Box<dyn ManagedTask>
        });
        let mut initialized =
            InitializedOutput::new(()).with_existing_data(!self.rows().is_empty());
        initialized.task = task;
        Ok(initialized)
    }

    async fn handle_data(
        &mut self,
        _: &mut (),
        batch: &BatchId,
        records: &[RecordResult<Event, E>],
    ) -> Result<(), BoxError> {
        if take_one(&self.failing_writes) {
            return Err(format!("{} write failure", self.name).into());
        }
        {
            let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
            let mut errors = self.errors.lock().unwrap_or_else(PoisonError::into_inner);
            for record in records {
                match record {
                    Ok(e) if e.kind == self.name || e.kind == "both" => rows.push(e.id.clone()),
                    Ok(_) => {}
                    Err(error) => errors.push(error.to_string()),
                }
            }
        }
        self.batches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(batch.key.clone());
        self.journal.push(format!("write {}", self.name));
        if take_one(&self.failing_after_writes) {
            return Err(format!("{} failure after write", self.name).into());
        }
        Ok(())
    }

    async fn already_written(&mut self, _: &mut (), batch: &BatchId) -> Result<bool, BoxError> {
        let written = self
            .batches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&batch.key);
        Ok(self.knows_written && written)
    }

    async fn terminate(&mut self, _: &mut ()) -> Result<(), BoxError> {
        self.journal.push(format!("terminate output {}", self.name));
        Ok(())
    }
}

// ---------------------------------------------------------------- middleware

/// Why [`Checked`] rejected an event.
#[derive(Debug, thiserror::Error)]
#[error("{id}: rejected")]
pub struct Rejected {
    pub id: String,
}

impl GlaconRecordError for Rejected {
    fn reason(&self) -> &'static str {
        "rejected"
    }
}

/// Rejects "reject" events and fails on "fail" ones; the rest pass
/// unchanged.
#[derive(Clone)]
pub struct Checked {
    pub input: String,
    pub journal: Journal,
}

#[async_trait]
impl GlaconCoreMiddleware<Event, Event> for Checked {
    type Error = Rejected;

    async fn init(&mut self, ctx: &MiddlewareContext) -> Result<(), BoxError> {
        self.input = ctx.input().to_string();
        self.journal.push(format!("init middleware {}", self.input));
        Ok(())
    }

    async fn handle_data(
        &mut self,
        _: &BatchId,
        events: Vec<Event>,
    ) -> Result<Vec<RecordResult<Event, Rejected>>, BoxError> {
        if events.iter().any(|e| e.kind == "fail") {
            return Err("middleware failure".into());
        }
        Ok(events
            .into_iter()
            .map(|e| match e.kind {
                "reject" => Err(Rejected { id: e.id }),
                _ => Ok(e),
            })
            .collect())
    }

    async fn terminate(&mut self) -> Result<(), BoxError> {
        self.journal
            .push(format!("terminate middleware {}", self.input));
        Ok(())
    }
}

// ---------------------------------------------------------------- routes

/// Settings of the route `name`: its ledger under `state`.
pub fn settings(state: &Path, name: &str) -> RouteSettings {
    RouteSettings::new(state.join(name))
}

/// The ledger of the route `name`, to check what it recorded once it ran.
pub async fn ledger(state: &Path, name: &str) -> Result<Ledger, Error> {
    Ledger::open(&state.join(name)).await
}

/// A route over `inputs` writing to `left` and `right` through [`Checked`],
/// its ledger under `state`.
pub fn route(
    name: &str,
    state: &Path,
    inputs: Vec<MemoryInput>,
    left: &MemoryOutput,
    right: &MemoryOutput,
    journal: &Journal,
) -> RouteOutputs<Event, Event, Rejected> {
    let builder = inputs.into_iter().fold(
        Route::builder(name, settings(state, name)),
        |builder, input| builder.input(input.name, input),
    );
    route_from(builder, left, right, journal)
}

/// The rest of [`route`], for a builder with other inputs.
pub fn route_from(
    builder: RouteBuilder<Event>,
    left: &MemoryOutput,
    right: &MemoryOutput,
    journal: &Journal,
) -> RouteOutputs<Event, Event, Rejected> {
    builder
        .middleware(Checked {
            input: String::new(),
            journal: journal.clone(),
        })
        .output(left.name, left.clone())
        .output(right.name, right.clone())
}

/// Run a task (a route, or a TaskManager of routes) until it ends.
pub async fn run(task: impl ManagedTask + 'static) -> TaskResult {
    run_until(task, std::future::pending()).await
}

/// Run a task until it ends or `shutdown` resolves, then stop it. Fails the
/// test after 30 seconds.
pub async fn run_until(
    task: impl ManagedTask + 'static,
    shutdown: impl Future<Output = ()>,
) -> TaskResult {
    let (trigger, listener) = triggered::trigger();
    let mut running = Box::new(task).start_task(listener);
    let run = async {
        tokio::select! {
            result = &mut running => result,
            _ = shutdown => {
                trigger.trigger();
                running.await
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("the route should stop")
}

/// The route's error behind a failed run, through every TaskManager layer.
pub fn route_error(err: &TaskError) -> &Error {
    let TaskError::Task(e) = err else {
        panic!("not a task error: {err}");
    };
    if let Some(nested) = e.downcast_ref::<TaskError>() {
        return route_error(nested);
    }
    e.downcast_ref::<Error>()
        .unwrap_or_else(|| panic!("not a glacon error: {e}"))
}

// ---------------------------------------------------------------- helpers

/// The `left` and `right` outputs.
pub fn outputs(journal: &Journal) -> (MemoryOutput, MemoryOutput) {
    (
        MemoryOutput::new(LEFT, journal),
        MemoryOutput::new(RIGHT, journal),
    )
}

/// Every stage stops after everything upstream of it: an input (dropped)
/// before its middleware terminates, every middleware before any output, and
/// every write of an output before it terminates.
pub fn assert_down_order(journal: &Journal) {
    let entries = journal.entries();
    let position = |entry: &str| entries.iter().position(|e| e == entry);
    for entry in &entries {
        if let Some(input) = entry.strip_prefix("drop input ") {
            let middleware = position(&format!("terminate middleware {input}"));
            assert!(
                middleware.is_some_and(|m| Some(m) > position(entry)),
                "middleware of {input} not terminated after it: {entries:?}"
            );
        }
    }
    journal.assert_before("terminate middleware", "terminate output");
    for output in [LEFT, RIGHT] {
        let terminated = journal.positions(&format!("terminate output {output}"));
        let writes = journal.positions(&format!("write {output}"));
        assert!(
            writes.iter().all(|w| terminated.iter().all(|t| w < t)),
            "{output} wrote after terminating: {entries:?}"
        );
    }
}

/// One input, `us`, producing batches `us.<n>` of one event each, kept by
/// both outputs.
pub fn us_input(
    batches: impl IntoIterator<Item = u32>,
    end: End,
    journal: &Journal,
) -> MemoryInput {
    let batches = batches
        .into_iter()
        .map(|n| batch(&format!("us.{n}"), vec![event(&format!("e{n}"), "both")]))
        .collect();
    MemoryInput::new("us", batches, end, journal)
}

/// The ledger of `route` has no batch of `us` in flight.
pub async fn assert_nothing_in_flight(state: &Path, route: &str) -> anyhow::Result<()> {
    let ledger = ledger(state, route).await?;
    assert_eq!(ledger.in_flight("us").await?, 0);
    Ok(())
}
