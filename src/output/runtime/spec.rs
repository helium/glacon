use super::ConnectedOutput;
use crate::Result;
use std::path::Path;

/// An output of a route before it is connected.
#[async_trait::async_trait]
pub(crate) trait OutputSpec<R>: Send + Sync {
    fn name(&self) -> &str;

    /// Connect the catalog, create the table if needed and start its writer.
    async fn connect(self: Box<Self>, spool_dir: &Path) -> Result<ConnectedOutput<R>>;
}
