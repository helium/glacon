use serde::Deserialize;
use std::time::Duration;

/// One Iceberg table a route writes to.
#[derive(Debug, Clone, Deserialize)]
pub struct OutputSettings {
    pub catalog: helium_iceberg::Settings,

    /// Rows buffered before a commit.
    #[serde(default = "default_max_batch_rows")]
    pub max_batch_rows: usize,

    /// Longest time rows wait before a commit.
    #[serde(default = "default_batch_timeout", with = "humantime_serde")]
    pub batch_timeout: Duration,
}

impl OutputSettings {
    pub fn new(catalog: helium_iceberg::Settings) -> Self {
        Self {
            catalog,
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
