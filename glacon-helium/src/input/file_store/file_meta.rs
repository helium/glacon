use chrono::{DateTime, Utc};

/// Where a decoded message came from.
#[derive(Debug, Clone, Copy)]
pub struct FileMeta<'a> {
    /// Object key of the file in its bucket.
    pub key: &'a str,
    /// Timestamp encoded in the file name.
    pub timestamp: DateTime<Utc>,
    /// Position of the message within the file, starting at 0.
    pub index: usize,
}
