use super::OutputContext;
use crate::{BatchId, BoxError, GlaconRecord, RecordResult};
use task_manager::ManagedTask;

/// An output: writes records somewhere (a table, a topic, ...).
///
/// The struct given to the route is the output's configuration. The route
/// calls `init` on it once, before any task starts, which builds the output's
/// [`State`](Self::State) (connections, writers, ...); every later call gets
/// both the configuration and that state.
///
/// Driven by the output's task for every batch, then terminated once every
/// processor stopped. An error stops the route.
///
/// An output gets every record of every batch, as the middleware left them:
/// `Ok` with a record, or `Err` with why the middleware could not make one
/// (see [`RecordResult`]). It keeps what it wants: deciding which records to
/// write, and what to do with the failures (write them somewhere, count
/// them, ignore them), is its job. That decision must depend on the result
/// alone, so a replayed batch gives the same outcome.
///
/// An output with nothing to set up has `()` as its state:
///
/// ```ignore
/// type State = ();
///
/// async fn init(&mut self, _: &OutputContext) -> Result<InitializedOutput<()>, BoxError> {
///     Ok(InitializedOutput::new(()))
/// }
/// ```
#[async_trait::async_trait]
pub trait GlaconCoreOutput<R: GlaconRecord>: Send + 'static {
    /// What `init` builds and the other calls write with.
    type State: Send + 'static;

    /// Prepare to write (connect, create the destination, ...): returns the
    /// output's state, the background work it needs, if any, and whether
    /// the destination already holds data.
    ///
    /// A returned task is supervised by the route: started before the output
    /// receives anything, stopped after the output terminated, and its
    /// failure fails the route. Several tasks can be bundled into one
    /// `TaskManager`. `init` must not spawn anything itself: what it returns
    /// may be dropped without ever starting, when another stage fails to
    /// initialize.
    async fn init(
        &mut self,
        ctx: &OutputContext,
    ) -> Result<InitializedOutput<Self::State>, BoxError>;

    /// Write what this output keeps of one batch (possibly nothing): one
    /// result per record the input read, in order. Return only once it is
    /// durable: returning `Ok` is the acknowledgement, after which the route
    /// never hands this output the batch again. An output keeping nothing of
    /// a batch still returns `Ok`; a record's `Err` is not this output's
    /// failure, and does not have to fail it.
    async fn handle_data(
        &mut self,
        state: &mut Self::State,
        batch: &BatchId,
        records: &[RecordResult<R>],
    ) -> Result<(), BoxError>;

    /// Whether this output already holds `batch`: asked only when a batch is
    /// read again after a crash, before writing it. An output that can tell
    /// (an idempotent key, a transactional sink) avoids writing it twice when
    /// the process died right after its write. Default: no.
    async fn already_written(
        &mut self,
        _state: &mut Self::State,
        _batch: &BatchId,
    ) -> Result<bool, BoxError> {
        Ok(false)
    }

    /// Flush what is buffered. Called once the output's task received its
    /// last batch, also after an error, and before its background task (if
    /// any) is stopped.
    async fn terminate(&mut self, _state: &mut Self::State) -> Result<(), BoxError> {
        Ok(())
    }
}

/// What an output's `init` returns.
pub struct InitializedOutput<S> {
    pub state: S,
    /// Background work for the route to supervise.
    pub task: Option<Box<dyn ManagedTask>>,
    /// The destination already held data. With an empty ledger, the route
    /// then refuses to start (see [`FreshState`](crate::FreshState)).
    pub has_data: bool,
}

impl<S> InitializedOutput<S> {
    /// `state`, without background work.
    pub fn new(state: S) -> Self {
        Self {
            state,
            task: None,
            has_data: false,
        }
    }

    /// With `task` as the background work for the route to supervise.
    pub fn with_task(mut self, task: impl ManagedTask + 'static) -> Self {
        self.task = Some(Box::new(task));
        self
    }

    /// Whether the destination already held data (default: no).
    pub fn with_existing_data(mut self, has_data: bool) -> Self {
        self.has_data = has_data;
        self
    }
}
