use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// One S3 bucket feeding a route.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FileStoreInputSettings {
    pub bucket: file_store::BucketSettings,

    /// Files at or before this time are ignored. Also the starting point on
    /// the first run.
    pub start_after: DateTime<Utc>,

    /// How often the bucket is listed (file_store default: 30s).
    #[serde(default, with = "humantime_serde")]
    pub poll_duration: Option<Duration>,

    /// How far before its resume point the poller re-lists, to catch late
    /// uploads (file_store default: 10 minutes). Keep it within the route's
    /// `processed_retention`: a done file forgotten, then listed again, is
    /// delivered again.
    #[serde(default, with = "humantime_serde")]
    pub offset: Option<Duration>,

    /// Stop polling after this long without new files (default: never). The
    /// route ends once all of its inputs have stopped.
    #[serde(default, with = "humantime_serde")]
    pub idle_timeout: Option<Duration>,
}

impl FileStoreInputSettings {
    pub fn new(bucket: file_store::BucketSettings, start_after: DateTime<Utc>) -> Self {
        Self {
            bucket,
            start_after,
            poll_duration: None,
            offset: None,
            idle_timeout: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_settings_parse_humantime() -> anyhow::Result<()> {
        let s: FileStoreInputSettings = serde_json::from_str(
            r#"{
                "bucket": {"bucket": "b"},
                "start_after": "2026-01-01T00:00:00Z",
                "poll_duration": "5s",
                "idle_timeout": "1m"
            }"#,
        )?;
        assert_eq!(s.poll_duration, Some(humantime::parse_duration("5s")?));
        assert_eq!(s.idle_timeout, Some(humantime::parse_duration("1 minute")?));
        assert_eq!(s.offset, None);
        Ok(())
    }

    #[test]
    fn serializing_leaves_out_the_bucket_keys() -> anyhow::Result<()> {
        let s: FileStoreInputSettings = serde_json::from_str(
            r#"{
                "bucket": {"bucket": "b", "access_key_id": "KEY-ID", "secret_access_key": "SECRET"},
                "start_after": "2026-01-01T00:00:00Z",
                "offset": "30m"
            }"#,
        )?;

        let printed = serde_json::to_string(&s)?;
        for secret in ["KEY-ID", "SECRET"] {
            assert!(!printed.contains(secret), "{secret} leaked: {printed}");
        }
        assert!(printed.contains(r#""offset":"30m""#), "{printed}");
        Ok(())
    }
}
