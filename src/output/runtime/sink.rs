use crate::Result;

/// A connected output: stages rows for the file being processed, then hands
/// them to its batched writer.
#[async_trait::async_trait]
pub(crate) trait OutputSink<R>: Send + Sync {
    fn name(&self) -> &str;

    /// Convert `record` into rows and stage them.
    fn stage(&mut self, record: &R);

    /// Queue the staged rows to the writer; returns once they are durable in
    /// its spool. Returns `(records, rows)` that were staged.
    async fn flush(&mut self) -> Result<(u64, u64)>;

    /// Drop the staged rows (the output already took this file).
    fn discard(&mut self);
}
