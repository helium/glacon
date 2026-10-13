use super::{Batch, InputContext};
use crate::{BoxError, GlaconRecord};

/// Produces a route's records, in batches.
///
/// The route calls `init` before any task starts, then `next_batch` from the
/// input's task until it returns `None` or the route stops, then
/// `terminate`. An error stops the route.
#[async_trait::async_trait]
pub trait GlaconCoreInput<R: GlaconRecord>: Send + 'static {
    /// Prepare to read, e.g. connect and start a poller.
    async fn init(&mut self, _ctx: &InputContext) -> Result<(), BoxError> {
        Ok(())
    }

    /// The next batch, or `None` once there is nothing left to read.
    ///
    /// When the route stops, a pending call is dropped: it must be cancel
    /// safe, or accept losing what it was reading.
    async fn next_batch(&mut self) -> Result<Option<Batch<R>>, BoxError>;

    /// Release what `init` set up. Called once the input's task stops
    /// reading, also after an error.
    async fn terminate(&mut self) -> Result<(), BoxError> {
        Ok(())
    }
}
