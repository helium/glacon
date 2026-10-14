use ::file_store::{
    FileInfo,
    file_info_poller::{FileInfoPollerState, FileInfoPollerStateRecorder},
};
use chrono::{DateTime, TimeDelta, Utc};
use glacon_core::InputLedger;

/// A [`FileStoreInput`](super::FileStoreInput)'s poller state: the route's
/// ledger, so there is one record of what each input read and delivered.
///
/// - resume point: the oldest file not delivered everywhere yet, else the
///   newest done one;
/// - a file is skipped once done (every output it was routed to has it);
/// - done files are forgotten once out of the poller's window and older than
///   the route's retention.
///
/// The poller's own `process_name` and file type are ignored: the ledger is
/// per input already.
#[derive(Debug, Clone)]
pub(super) struct LedgerState {
    pub ledger: InputLedger,
}

fn to_file_store(error: glacon_core::Error) -> ::file_store::Error {
    ::file_store::Error::ExternalError(Box::new(error))
}

#[async_trait::async_trait]
impl FileInfoPollerState for LedgerState {
    /// The poller lists files strictly after its resume point (minus its
    /// offset): step back a millisecond so a file still in flight there is
    /// listed again, even with no offset.
    async fn latest_timestamp(
        &self,
        _process_name: &str,
        _file_type: &str,
    ) -> ::file_store::Result<Option<DateTime<Utc>>> {
        let resume = self.ledger.resume_point().await.map_err(to_file_store)?;
        Ok(resume.map(|t| t - TimeDelta::milliseconds(1)))
    }

    async fn exists(
        &self,
        _process_name: &str,
        file_info: &FileInfo,
    ) -> ::file_store::Result<bool> {
        self.ledger
            .is_done(&file_info.key)
            .await
            .map_err(to_file_store)
    }

    async fn clean(
        &self,
        _process_name: &str,
        _file_type: &str,
        offset: DateTime<Utc>,
    ) -> ::file_store::Result<u64> {
        self.ledger.prune(offset).await.map_err(to_file_store)
    }
}

/// The route records what was read in its ledger: file_store's own recording
/// is skipped.
pub(super) struct NoRecord;

#[async_trait::async_trait]
impl FileInfoPollerStateRecorder for NoRecord {
    async fn record(&mut self, _process_name: &str, _file_info: &FileInfo) -> ::file_store::Result {
        Ok(())
    }
}
