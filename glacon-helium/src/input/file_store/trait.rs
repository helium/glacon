use super::FileMeta;
use glacon_core::GlaconRecord;

/// Turns the protobuf messages of one kind of file into the route's record.
///
/// The bucket an input reads from is given by its `FileStoreInputSettings`
/// when it is wrapped in a [`FileStoreInput`](super::FileStoreInput); the
/// same `GlaconInput` can therefore be used for several buckets.
pub trait GlaconInput: Send + Sync + 'static {
    /// Message type stored (length-delimited, gzipped) in the files.
    type Proto: prost::Message + Default + Clone + Send + Sync + 'static;
    /// Record produced; must match the route's record type.
    type Record: GlaconRecord;

    /// File name prefix to poll (e.g. `"cdr_report"`), without the trailing dot.
    fn prefix(&self) -> &str;

    /// Decode one message. Returning `None` drops the message; the route logs
    /// it at error level and counts it in `glacon_decode_failures_total`.
    fn decode(&self, proto: Self::Proto, file: FileMeta<'_>) -> Option<Self::Record>;
}
