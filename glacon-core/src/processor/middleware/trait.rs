use super::{Destination, MiddlewareContext};
use crate::{BoxError, GlaconRecord};

/// Decides which outputs, if any, receive each record.
///
/// Every input's processor gets its own instance, created by the route's
/// middleware factory (see
/// [`RouteBuilder::middleware_with`](crate::RouteBuilder::middleware_with),
/// which hands it the [`OutputId`](super::OutputId)s of the outputs). The
/// route calls `init` before any task starts, `handle_data` for every batch
/// of the input, then `terminate` once the processor stops. An error stops
/// the route.
///
/// Deduplication, verification and the like live here: drop a record by
/// returning [`Destination::Drop`] for it.
#[async_trait::async_trait]
pub trait GlaconCoreMiddleware<R: GlaconRecord>: Send + 'static {
    async fn init(&mut self, _ctx: &MiddlewareContext) -> Result<(), BoxError> {
        Ok(())
    }

    /// Where each record of a batch goes: one [`Destination`] per record, in
    /// the same order.
    async fn handle_data(&mut self, records: &[R]) -> Result<Vec<Destination>, BoxError>;

    async fn terminate(&mut self) -> Result<(), BoxError> {
        Ok(())
    }
}
