use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};

/// Route-wide settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RouteSettings {
    /// Directory holding the route's ledger (`state.db`). MUST be durable (a
    /// mounted volume): it is the only record of what was delivered.
    pub state_dir: PathBuf,

    /// How long done batches are remembered. Must cover how far back an
    /// input may produce a batch again (a file store poller's offset, for
    /// example): a forgotten batch seen again is delivered again. Each
    /// input's newest done batch is always kept.
    #[serde(default = "default_retention", with = "humantime_serde")]
    pub processed_retention: Duration,
}

impl RouteSettings {
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: state_dir.into(),
            processed_retention: default_retention(),
        }
    }
}

fn default_retention() -> Duration {
    humantime::parse_duration("3 days").expect("valid duration literal")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_retention_is_three_days() -> anyhow::Result<()> {
        let s: RouteSettings = serde_json::from_str(r#"{"state_dir": "/tmp/x"}"#)?;
        assert_eq!(s.processed_retention, humantime::parse_duration("3 days")?);
        Ok(())
    }
}
