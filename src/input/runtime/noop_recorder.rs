use file_store::{FileInfo, file_info_poller::FileInfoPollerStateRecorder};

/// Recorder handed to `file_store` that records nothing: a file is marked
/// processed by the route itself, once every output took it.
pub(crate) struct NoopRecorder;

#[async_trait::async_trait]
impl FileInfoPollerStateRecorder for NoopRecorder {
    async fn record(&mut self, _process_name: &str, _file_info: &FileInfo) -> file_store::Result {
        Ok(())
    }
}
