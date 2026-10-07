use crate::Result;
use file_store::FileInfo;
use futures::stream::BoxStream;
use std::sync::Arc;

/// One polled file, decoded into records.
pub(crate) struct DecodedFile<R> {
    pub input: Arc<str>,
    pub file: FileInfo,
    pub records: Vec<R>,
}

/// Decoded files of every input of a route, merged.
pub(crate) type DecodedFiles<R> = BoxStream<'static, Result<DecodedFile<R>>>;
