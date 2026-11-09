use super::MiddlewareContext;
use crate::{BatchId, BoxError, GlaconRecord};

/// Transforms the records of every batch before the outputs get them: from
/// what inputs produce (`In`) to what outputs write (`Out`).
///
/// Every input's processor gets its own instance, cloned from the one given
/// to [`RouteBuilder::middleware`](crate::RouteBuilder::middleware). The
/// route calls `init` before any task starts, `handle_data` for every batch
/// of the input, then `terminate` once the processor stops. An error stops
/// the route.
///
/// A middleware does not pick outputs: every output gets every record and
/// keeps what it wants. A decision several outputs depend on (is this record
/// valid?) is made here once and set on the record, for each output to
/// filter on. A record that should reach no output (malformed, a duplicate)
/// is dropped by leaving it out. A middleware must not add records.
///
/// After a crash, a batch that was not done is handled again, and must give
/// the same records. A middleware keeping state across batches (seen keys,
/// for example) must be replay safe: key that state by the batch's `input`
/// and `key` (not the whole [`BatchId`], whose timestamp may differ on a
/// replay), so a record its own batch already claimed is still kept on a
/// replay.
#[async_trait::async_trait]
pub trait GlaconCoreMiddleware<In: GlaconRecord, Out: GlaconRecord>: Send + 'static {
    async fn init(&mut self, _ctx: &MiddlewareContext) -> Result<(), BoxError> {
        Ok(())
    }

    /// Transform the records of one batch. Leave a record out to drop it.
    async fn handle_data(
        &mut self,
        batch: &BatchId,
        records: Vec<In>,
    ) -> Result<Vec<Out>, BoxError>;

    async fn terminate(&mut self) -> Result<(), BoxError> {
        Ok(())
    }
}
