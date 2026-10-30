use super::OutputContext;
use crate::{BatchId, BoxError, GlaconRecord};
use task_manager::ManagedTask;

/// An output as given to the route: its configuration. The route turns it
/// into a running [`GlaconCoreOutput`] with `init`, before any task starts.
///
/// An output with nothing to set up can be its own running output:
///
/// ```ignore
/// async fn init(self: Box<Self>, _: &OutputContext) -> Result<InitializedOutput<R>, BoxError> {
///     Ok(InitializedOutput::new(*self))
/// }
/// ```
#[async_trait::async_trait]
pub trait GlaconCoreOutputInit<R: GlaconRecord>: Send + 'static {
    /// Prepare to write (connect, create the destination, ...): returns the
    /// running output its task drives, and the background work it needs, if
    /// any.
    ///
    /// A returned task is supervised by the route: started before the output
    /// receives anything, stopped after the output terminated, and its
    /// failure fails the route. Several tasks can be bundled into one
    /// `TaskManager`. `init` must not spawn anything itself: what it returns
    /// may be dropped without ever starting, when another stage fails to
    /// initialize.
    async fn init(self: Box<Self>, ctx: &OutputContext) -> Result<InitializedOutput<R>, BoxError>;
}

/// What an output's `init` returns.
pub struct InitializedOutput<R> {
    pub output: Box<dyn GlaconCoreOutput<R>>,
    /// Background work for the route to supervise.
    pub task: Option<Box<dyn ManagedTask>>,
    /// The destination already held data. With an empty ledger, the route
    /// then refuses to start (see [`FreshState`](crate::FreshState)).
    pub has_data: bool,
}

impl<R: GlaconRecord> InitializedOutput<R> {
    /// `output`, without background work.
    pub fn new(output: impl GlaconCoreOutput<R>) -> Self {
        Self {
            output: Box::new(output),
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

/// A running output: writes records somewhere (a table, a topic, ...).
/// Driven by the output's task for every batch, then terminated once every
/// processor stopped. An error stops the route.
///
/// An output gets every record of every batch, as the middleware left them,
/// and keeps what it wants: deciding which records to drop is its job. That
/// decision must depend on the record alone, so a replayed batch gives the
/// same result.
#[async_trait::async_trait]
pub trait GlaconCoreOutput<R: GlaconRecord>: Send + 'static {
    /// Write the records of one batch this output keeps (possibly none).
    /// Return only once they are durable: returning `Ok` is the
    /// acknowledgement, after which the route never hands this output the
    /// batch again. An output keeping nothing of a batch still returns `Ok`.
    async fn handle_data(&mut self, batch: &BatchId, records: &[R]) -> Result<(), BoxError>;

    /// Whether this output already holds `batch`: asked only when a batch is
    /// read again after a crash, before writing it. An output that can tell
    /// (an idempotent key, a transactional sink) avoids writing it twice when
    /// the process died right after its write. Default: no.
    async fn already_written(&mut self, _batch: &BatchId) -> Result<bool, BoxError> {
        Ok(false)
    }

    /// Flush what is buffered. Called once the output's task received its
    /// last batch, also after an error, and before its background task (if
    /// any) is stopped.
    async fn terminate(&mut self) -> Result<(), BoxError> {
        Ok(())
    }
}
