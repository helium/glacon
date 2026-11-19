use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};

/// One Iceberg table a route writes to.
#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializing_leaves_out_the_catalog_and_s3_secrets() -> anyhow::Result<()> {
        let s: IcebergOutputSettings = serde_json::from_str(
            r#"{
                "catalog": {
                    "catalog_uri": "http://catalog",
                    "catalog_name": "mobile",
                    "auth": {"credential": "id:CATALOG-SECRET", "token": "TOKEN-SECRET"},
                    "s3": {"access_key_id": "S3-KEY-ID", "secret_access_key": "S3-SECRET"}
                },
                "spool_dir": "/var/spool",
                "batch_timeout": "5m"
            }"#,
        )?;

        let printed = serde_json::to_string(&s)?;
        for secret in ["CATALOG-SECRET", "TOKEN-SECRET", "S3-KEY-ID", "S3-SECRET"] {
            assert!(!printed.contains(secret), "{secret} leaked: {printed}");
        }
        assert!(printed.contains(r#""batch_timeout":"5m""#), "{printed}");
        Ok(())
    }
}
