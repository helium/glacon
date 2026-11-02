use super::{Batch, InputContext};
use crate::{BoxError, GlaconRecord};
use task_manager::ManagedTask;

/// An input: where a route's records come from, one batch at a time.
///
/// The struct given to the route is the input's configuration. The route
/// calls `init` on it once, before any task starts, which builds the input's
/// [`State`](Self::State) (connections, channels, ...); every later call gets
/// both the configuration and that state.
///
/// An input must be **replayable**. The route records every batch it reads
/// in its ledger and only marks it done once every output has it. After a
/// crash, the input must produce again the batches that are not done: resume
/// from [`InputContext::ledger`]'s `resume_point`, and skip what its
/// `is_done` says (the route also skips done batches itself). A source that
/// can't produce a batch again would lose it.
///
/// Driven by the input's task until `next_batch` returns `None` or the route
/// stops; dropped, with its state, when the input stops. An error stops the
/// route.
///
/// An input with nothing to set up has `()` as its state:
///
/// ```ignore
/// type State = ();
///
/// async fn init(&mut self, _: &InputContext) -> Result<InitializedInput<()>, BoxError> {
///     Ok(InitializedInput::new(()))
/// }
/// ```
#[async_trait::async_trait]
pub trait GlaconCoreInput<R: GlaconRecord>: Send + 'static {
    /// What `init` builds and `next_batch` reads from.
    type State: Send + 'static;

    /// Prepare to read (connect, build a poller, ...): returns the input's
    /// state, and the background work it needs, if any.
    ///
    /// A returned task is supervised by the route: started before the input
    /// is read, stopped after the input stops reading, and its failure fails
    /// the route. Several tasks can be bundled into one `TaskManager`. `init`
    /// must not spawn anything itself: what it returns may be dropped without
    /// ever starting, when another stage fails to initialize.
    async fn init(&mut self, ctx: &InputContext)
    -> Result<InitializedInput<Self::State>, BoxError>;

    /// The next batch, or `None` once there is nothing left to read.
    ///
    /// When the route stops, a pending call is dropped: it must be cancel
    /// safe, or accept losing what it was reading.
    async fn next_batch(&mut self, state: &mut Self::State) -> Result<Option<Batch<R>>, BoxError>;
}

/// What an input's `init` returns.
pub struct InitializedInput<S> {
    pub state: S,
    /// Background work for the route to supervise.
    pub task: Option<Box<dyn ManagedTask>>,
}

impl<S> InitializedInput<S> {
    /// `state`, without background work.
    pub fn new(state: S) -> Self {
        Self { state, task: None }
    }

    /// With `task` as the background work for the route to supervise.
    pub fn with_task(mut self, task: impl ManagedTask + 'static) -> Self {
        self.task = Some(Box::new(task));
        self
    }
}
