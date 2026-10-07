use serde::Deserialize;
use std::{path::PathBuf, time::Duration};

/// Route-wide settings.
#[derive(Debug, Clone, Deserialize)]
pub struct RouteSettings {
    /// Directory holding the route's `state.db` and its output spools. MUST be
    /// durable (a mounted volume): it is the only record of what was
    /// processed.
    pub state_dir: PathBuf,

    /// How long a dedup key is remembered after it was last seen.
    #[serde(default = "default_retention", with = "humantime_serde")]
    pub dedup_retention: Duration,

    /// How long processed-file records are kept (always at least the poller's
    /// lookback window).
    #[serde(default = "default_retention", with = "humantime_serde")]
    pub files_retention: Duration,
}

impl RouteSettings {
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: state_dir.into(),
            dedup_retention: default_retention(),
            files_retention: default_retention(),
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
    fn route_settings_default_retention_is_three_days() -> anyhow::Result<()> {
        let s: RouteSettings = serde_json::from_str(r#"{"state_dir": "/tmp/x"}"#)?;
        let three_days = humantime::parse_duration("3 days")?;
        assert_eq!(s.dedup_retention, three_days);
        assert_eq!(s.files_retention, three_days);
        Ok(())
    }
}
