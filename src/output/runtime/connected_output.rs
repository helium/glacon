use super::OutputSink;
use crate::tasks::NamedTask;

/// An output once connected: where the processor stages rows, and the writer
/// task committing them.
pub(crate) struct ConnectedOutput<R> {
    pub sink: Box<dyn OutputSink<R>>,
    pub writer: NamedTask,
    /// `namespace.table`, for logs and errors.
    pub table: String,
    /// The table already existed and had at least one snapshot.
    pub had_data: bool,
}
