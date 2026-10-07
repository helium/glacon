use super::DecodedFiles;
use crate::{Result, state::StateDb, tasks::NamedTask};
use std::{sync::Arc, time::Duration};

/// An input of a route with its record type fixed and its message type erased.
#[async_trait::async_trait]
pub(crate) trait InputSpec<R>: Send + Sync {
    fn name(&self) -> &str;

    /// Start polling: returns the stream of decoded files and the poller task.
    async fn start(
        self: Box<Self>,
        route: &str,
        db: Arc<StateDb>,
        files_retention: Duration,
    ) -> Result<(DecodedFiles<R>, NamedTask)>;
}
