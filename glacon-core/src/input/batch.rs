use chrono::{DateTime, Utc};

/// What an input produces (an input file, for example): records handed to
/// the middleware together.
#[derive(Debug, Clone)]
pub struct Batch<R> {
    /// Identifies the batch within its input, for logs.
    pub key: String,
    /// When the batch was produced; drives the input's lag metric.
    pub timestamp: DateTime<Utc>,
    pub records: Vec<R>,
}
