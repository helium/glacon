use chrono::{DateTime, Utc};
use std::sync::Arc;

/// What an input produces (an input file, for example): records handed to
/// the middleware together.
#[derive(Debug, Clone)]
pub struct Batch<R> {
    /// Identifies the batch within its input: the ledger tracks it under
    /// this key, so it must be stable when the batch is produced again.
    pub key: String,
    /// When the batch was produced; drives the input's resume point and lag
    /// metric.
    pub timestamp: DateTime<Utc>,
    pub records: Vec<R>,
}

/// A batch on its way from its input to its processor, recorded in the
/// ledger.
pub(crate) struct ReadBatch<R> {
    pub id: BatchId,
    /// Read before by a run that did not finish it.
    pub replayed: bool,
    pub records: Vec<R>,
}

/// Which batch, of which input, records came from. Handed to middlewares and
/// outputs with every batch's records, and how the ledger tracks it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchId {
    pub input: Arc<str>,
    pub key: String,
    pub timestamp: DateTime<Utc>,
}
