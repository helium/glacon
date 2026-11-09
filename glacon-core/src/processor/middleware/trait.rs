use super::MiddlewareContext;
use crate::{BatchId, BoxError, GlaconRecord, RecordResult};

/// Transforms the records of every batch before the outputs get them: from
/// what inputs produce (`In`) to what outputs write (`Out`).
///
/// Every input's processor gets its own instance, cloned from the one given
/// to [`RouteBuilder::middleware`](crate::RouteBuilder::middleware). The
/// route calls `init` before any task starts, `handle_data` for every batch
/// of the input, then `terminate` once the processor stops. An error stops
/// the route.
///
/// A middleware does not pick outputs, nor drop records: it returns one
/// [`RecordResult`] per input record, in order, and every output gets all of
/// them. A record that should reach no output as data (malformed, a
/// duplicate) becomes an `Err` saying why; each output decides what to do
/// with it. A decision several outputs depend on (is this record valid?) is
/// made here once, for each output to act on.
///
/// After a crash, a batch that was not done is handled again, and must give
/// the same results. A middleware keeping state across batches (seen keys,
/// for example) must be replay safe: key that state by [`BatchId`], so a
/// record its own batch already claimed is still `Ok` on a replay.
#[async_trait::async_trait]
pub trait GlaconCoreMiddleware<In: GlaconRecord, Out: GlaconRecord>: Send + 'static {
    async fn init(&mut self, _ctx: &MiddlewareContext) -> Result<(), BoxError> {
        Ok(())
    }

    /// Transform the records of one batch: exactly one result per record,
    /// in the same order. Returning another count fails the route.
    ///
    /// An `Err` in the list is that record's failure, handed to the outputs;
    /// the outer `Err` is the middleware's own failure, and stops the route.
    async fn handle_data(
        &mut self,
        batch: &BatchId,
        records: Vec<In>,
    ) -> Result<Vec<RecordResult<Out>>, BoxError>;

    async fn terminate(&mut self) -> Result<(), BoxError> {
        Ok(())
    }
}
