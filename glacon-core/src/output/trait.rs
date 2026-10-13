use super::OutputContext;
use crate::{BoxError, GlaconRecord};

/// Writes the records routed to one output (a table, a topic, ...).
///
/// The route calls `init` before any task starts, `handle_data` from the
/// output's task for every batch routed to it, then `terminate` once every
/// processor stopped. An error stops the route.
#[async_trait::async_trait]
pub trait GlaconCoreOutput<R: GlaconRecord>: Send + 'static {
    /// Prepare to write, e.g. connect and create the destination.
    async fn init(&mut self, _ctx: &OutputContext) -> Result<(), BoxError> {
        Ok(())
    }

    /// Write the records of one batch routed to this output.
    async fn handle_data(&mut self, records: &[&R]) -> Result<(), BoxError>;

    /// Flush what is buffered and release what `init` set up. Called once
    /// the output's task received its last batch, also after an error.
    async fn terminate(&mut self) -> Result<(), BoxError> {
        Ok(())
    }
}
