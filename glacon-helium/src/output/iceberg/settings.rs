use serde::Deserialize;
use std::{path::PathBuf, time::Duration};

/// One Iceberg table a route writes to.
#[derive(Debug, Clone, Deserialize)]
pub struct IcebergOutputSettings {
    pub catalog: helium_iceberg::Settings,

    /// Durable directory for the rows written but not committed yet. Each
    /// output spools to `<spool_dir>/<route>/<output>`, so outputs can share
    /// it.
    pub spool_dir: PathBuf,

    /// Rows buffered before a commit.
    #[serde(default = "default_max_batch_rows")]
    pub max_batch_rows: usize,

    /// Longest time rows wait before a commit.
    #[serde(default = "default_batch_timeout", with = "humantime_serde")]
    pub batch_timeout: Duration,
}

impl IcebergOutputSettings {
    pub fn new(catalog: helium_iceberg::Settings, spool_dir: impl Into<PathBuf>) -> Self {
        Self {
            catalog,
            spool_dir: spool_dir.into(),
            max_batch_rows: default_max_batch_rows(),
            batch_timeout: default_batch_timeout(),
        }
    }
}

fn default_max_batch_rows() -> usize {
    10_000
}

fn default_batch_timeout() -> Duration {
    humantime::parse_duration("15 minutes").expect("valid duration literal")
}
