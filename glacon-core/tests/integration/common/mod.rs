//! In-memory inputs, outputs and middleware. A [`Journal`] shared by
//! everything in a test records every hook call, in order, so tests can
//! assert on the Up and Down order.

use chrono::Utc;
use glacon_core::{
    Batch, BoxError, Destination, Error, GlaconCoreInput, GlaconCoreMiddleware, GlaconCoreOutput,
    InputContext, MiddlewareContext, OutputContext, OutputId, Route, RouteBuilder, async_trait,
    task_manager::{ManagedTask, TaskError, TaskResult},
};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub const LEFT: &str = "left";
pub const RIGHT: &str = "right";

// ---------------------------------------------------------------- record

#[derive(Debug, Clone)]
pub struct Event {
    pub id: String,
    /// "left", "right", "both", "drop" or "fail" (see [`ByKind`]).
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

// ---------------------------------------------------------------- input

/// Hands out its batches, then returns, fails, panics or waits to be
/// stopped.
pub struct MemoryInput {
    pub name: &'static str,
    pub batches: VecDeque<Batch<Event>>,
    pub end: End,
    pub fail_init: bool,
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
            journal: journal.clone(),
        }
    }
}

#[async_trait]
impl GlaconCoreInput<Event> for MemoryInput {
    async fn init(&mut self, ctx: &InputContext) -> Result<(), BoxError> {
        assert_eq!(ctx.input(), self.name);
        if self.fail_init {
            return Err("input init failure".into());
        }
        self.journal.push(format!("init input {}", self.name));
        Ok(())
    }

    async fn next_batch(&mut self) -> Result<Option<Batch<Event>>, BoxError> {
        if let Some(batch) = self.batches.pop_front() {
            return Ok(Some(batch));
        }
        match self.end {
            End::Return => Ok(None),
            End::HoldOpen => std::future::pending().await,
            End::Fail => Err("input failure".into()),
            End::Panic => panic!("input panic"),
        }
    }

    async fn terminate(&mut self) -> Result<(), BoxError> {
        self.journal.push(format!("terminate input {}", self.name));
        Ok(())
    }
}

// ---------------------------------------------------------------- output

/// Keeps the ids it was given.
#[derive(Clone)]
pub struct MemoryOutput {
    pub name: &'static str,
    rows: Arc<Mutex<Vec<String>>>,
    /// Writes left to fail.
    failing_writes: Arc<AtomicUsize>,
    pub fail_init: bool,
    journal: Journal,
}

impl MemoryOutput {
    pub fn new(name: &'static str, journal: &Journal) -> Self {
        Self {
            name,
            rows: Default::default(),
            failing_writes: Default::default(),
            fail_init: false,
            journal: journal.clone(),
        }
    }

    /// Fail the next `n` writes.
    pub fn fail_writes(&self, n: usize) {
        self.failing_writes.store(n, Ordering::SeqCst);
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

#[async_trait]
impl GlaconCoreOutput<Event> for MemoryOutput {
    async fn init(&mut self, ctx: &OutputContext) -> Result<(), BoxError> {
        assert_eq!(ctx.output(), self.name);
        if self.fail_init {
            return Err("output init failure".into());
        }
        self.journal.push(format!("init output {}", self.name));
        Ok(())
    }

    async fn handle_data(&mut self, records: &[&Event]) -> Result<(), BoxError> {
        let fail = self
            .failing_writes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if fail {
            return Err(format!("{} write failure", self.name).into());
        }
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(records.iter().map(|e| e.id.clone()));
        self.journal.push(format!("write {}", self.name));
        Ok(())
    }

    async fn terminate(&mut self) -> Result<(), BoxError> {
        self.journal.push(format!("terminate output {}", self.name));
        Ok(())
    }
}

// ---------------------------------------------------------------- middleware

/// left -> left, right -> right, both -> every output, drop -> nowhere,
/// fail -> error.
pub struct ByKind {
    pub left: OutputId,
    pub right: OutputId,
    pub input: String,
    pub journal: Journal,
}

#[async_trait]
impl GlaconCoreMiddleware<Event> for ByKind {
    async fn init(&mut self, ctx: &MiddlewareContext) -> Result<(), BoxError> {
        self.input = ctx.input().to_string();
        self.journal.push(format!("init middleware {}", self.input));
        Ok(())
    }

    async fn handle_data(&mut self, events: &[Event]) -> Result<Vec<Destination>, BoxError> {
        events
            .iter()
            .map(|event| match event.kind {
                "left" => Ok(Destination::Only(self.left.into())),
                "right" => Ok(Destination::Only(self.right.into())),
                "both" => Ok(Destination::All),
                "fail" => Err("middleware failure".into()),
                _ => Ok(Destination::Drop),
            })
            .collect()
    }

    async fn terminate(&mut self) -> Result<(), BoxError> {
        self.journal
            .push(format!("terminate middleware {}", self.input));
        Ok(())
    }
}

// ---------------------------------------------------------------- routes

/// A route over `inputs` writing to `left` and `right` through [`ByKind`].
pub fn route(
    name: &str,
    inputs: Vec<MemoryInput>,
    left: &MemoryOutput,
    right: &MemoryOutput,
    journal: &Journal,
) -> RouteBuilder<Event> {
    let journal = journal.clone();
    inputs
        .into_iter()
        .fold(Route::builder(name), |builder, input| {
            builder.input(input.name, input)
        })
        .output(left.name, left.clone())
        .output(right.name, right.clone())
        .middleware_with(move |outputs| {
            Ok(ByKind {
                left: outputs.id(LEFT)?,
                right: outputs.id(RIGHT)?,
                input: String::new(),
                journal: journal.clone(),
            })
        })
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
