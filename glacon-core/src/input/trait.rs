use super::{Batch, InputContext};
use crate::{BoxError, GlaconRecord};
use task_manager::ManagedTask;

/// An input as given to the route: its configuration. The route turns it
/// into a running [`GlaconCoreInput`] with `init`, before any task starts.
///
/// An input must be **replayable**. The route records every batch it reads
/// in its ledger and only marks it done once every output has it. After a crash, the input must produce again the batches that are
/// not done: resume from [`InputContext::ledger`]'s `resume_point`, and skip
/// what its `is_done` says (the route also skips done batches itself). A
/// source that can't produce a batch again would lose it.
///
/// An input with nothing to set up can be its own running input:
///
/// ```ignore
/// async fn init(self: Box<Self>, _: &InputContext) -> Result<InitializedInput<R>, BoxError> {
///     Ok(InitializedInput::new(*self))
/// }
/// ```
#[async_trait::async_trait]
pub trait GlaconCoreInputInit<R: GlaconRecord>: Send + 'static {
    /// Prepare to read (connect, build a poller, ...): returns the running
    /// input its task drives, and the background work it needs, if any.
    ///
    /// A returned task is supervised by the route: started before the input
    /// is read, stopped after the input stops reading, and its failure fails
    /// the route. Several tasks can be bundled into one `TaskManager`. `init`
    /// must not spawn anything itself: what it returns may be dropped without
    /// ever starting, when another stage fails to initialize.
    async fn init(self: Box<Self>, ctx: &InputContext) -> Result<InitializedInput<R>, BoxError>;
}

/// What an input's `init` returns.
pub struct InitializedInput<R> {
    pub input: Box<dyn GlaconCoreInput<R>>,
    /// Background work for the route to supervise.
    pub task: Option<Box<dyn ManagedTask>>,
}

impl<R: GlaconRecord> InitializedInput<R> {
    /// `input`, without background work.
    pub fn new(input: impl GlaconCoreInput<R>) -> Self {
        Self {
            input: Box::new(input),
            task: None,
        }
    }

    /// With `task` as the background work for the route to supervise.
    pub fn with_task(mut self, task: impl ManagedTask + 'static) -> Self {
        self.task = Some(Box::new(task));
        self
    }
}

/// A running input: produces its batches. Driven by the input's task until
/// it returns `None` or the route stops; dropped when the input stops. An
/// error stops the route.
#[async_trait::async_trait]
pub trait GlaconCoreInput<R: GlaconRecord>: Send + 'static {
    /// The next batch, or `None` once there is nothing left to read.
    ///
    /// When the route stops, a pending call is dropped: it must be cancel
    /// safe, or accept losing what it was reading.
    async fn next_batch(&mut self) -> Result<Option<Batch<R>>, BoxError>;
}
