use crate::{metrics, state::StateDb};
use chrono::{DateTime, Utc};
use file_store::{FileInfo, file_info_poller::FileInfoPollerState};
use std::{sync::Arc, time::Duration};

/// file_store poller state backed by the route's `processed_files` table.
/// The poller's process name is the input name.
pub(crate) struct PollerState {
    pub route: String,
    pub db: Arc<StateDb>,
    pub retention: Duration,
}

#[async_trait::async_trait]
impl FileInfoPollerState for PollerState {
    async fn latest_timestamp(
        &self,
        process_name: &str,
        _file_type: &str,
    ) -> file_store::Result<Option<DateTime<Utc>>> {
        self.db
            .latest_file_timestamp(process_name)
            .await
            .map_err(to_file_store_error)
    }

    async fn exists(&self, process_name: &str, file_info: &FileInfo) -> file_store::Result<bool> {
        self.db
            .is_file_processed(process_name, &file_info.key)
            .await
            .map_err(to_file_store_error)
    }

    /// `offset` is the start of the poller's listing window; rows inside it
    /// are still needed to skip re-listed files, so only rows older than both
    /// the window and the retention go.
    async fn clean(
        &self,
        process_name: &str,
        _file_type: &str,
        offset: DateTime<Utc>,
    ) -> file_store::Result<u64> {
        let before = offset.min(Utc::now() - self.retention);
        let removed = self
            .db
            .clean_processed_files(process_name, before)
            .await
            .map_err(to_file_store_error)?;

        metrics::state_pruned(&self.route, "processed_files", removed);

        Ok(removed)
    }
}

fn to_file_store_error(err: crate::Error) -> file_store::Error {
    file_store::Error::from(std::io::Error::other(err.to_string()))
}
