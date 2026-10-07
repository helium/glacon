//! Test environment for routes against the docker compose services (RustFS,
//! Polaris, Trino).
//!
//! Each [`Harness`] gets its own Iceberg catalog, its own buckets and its own
//! state directories, so tests can run in parallel.

use chrono::{DateTime, Utc};
use file_store::{BucketSettings, aws_local::AwsLocal};
use glacon::{InputSettings, OutputSettings, RouteSettings, file_store, helium_iceberg};
use helium_iceberg::IcebergTestHarness;
use std::{path::PathBuf, time::Duration};

/// Region and credentials `AwsLocal` uses for its buckets.
const S3_REGION: &str = "us-east-1";
const S3_CREDENTIAL: &str = "admin";

const CATALOG_ATTEMPTS: u32 = 5;

pub struct Harness {
    pub iceberg: IcebergTestHarness,
    buckets: Vec<AwsLocal>,
    state_root: tempfile::TempDir,
    /// Test-friendly timings, parsed once.
    poll_duration: Duration,
    idle_timeout: Duration,
    batch_timeout: Duration,
}

impl Harness {
    /// A fresh catalog (registered in Trino) and an empty state root.
    ///
    /// Polaris intermittently answers 500 when many catalogs are created at
    /// once (parallel tests), so creation is retried; each attempt uses a new
    /// catalog name.
    pub async fn new() -> anyhow::Result<Self> {
        let mut attempt = 0;
        let iceberg = loop {
            attempt += 1;
            match IcebergTestHarness::new().await {
                Ok(iceberg) => break iceberg,
                Err(err) if attempt < CATALOG_ATTEMPTS => {
                    tracing::warn!(attempt, %err, "creating test catalog failed, retrying");
                    tokio::time::sleep(humantime::parse_duration("250ms")? * attempt).await;
                }
                Err(err) => return Err(err.into()),
            }
        };
        Ok(Self {
            iceberg,
            buckets: Vec::new(),
            state_root: tempfile::tempdir()?,
            poll_duration: humantime::parse_duration("100ms")?,
            idle_timeout: humantime::parse_duration("2s")?,
            batch_timeout: humantime::parse_duration("1s")?,
        })
    }

    /// Create a new, empty bucket. Removed by [`Harness::cleanup`].
    pub async fn add_bucket(&mut self) -> anyhow::Result<usize> {
        let bucket = AwsLocal::new().await;
        bucket.create_bucket().await?;
        self.buckets.push(bucket);
        Ok(self.buckets.len() - 1)
    }

    /// Upload one gzipped, length-delimited file of `messages` named
    /// `<prefix>.<timestamp>.gz`.
    pub async fn put_file<M: prost::Message>(
        &self,
        bucket: usize,
        prefix: &str,
        timestamp: DateTime<Utc>,
        messages: Vec<M>,
    ) -> anyhow::Result<String> {
        Ok(self.buckets[bucket]
            .put_protos_at_time(prefix, messages, timestamp)
            .await?)
    }

    /// Input settings for `bucket`: polls every 100ms and stops after 2s
    /// without new files, so routes end on their own.
    pub fn input_settings(&self, bucket: usize, start_after: DateTime<Utc>) -> InputSettings {
        let bucket = &self.buckets[bucket];
        InputSettings {
            bucket: BucketSettings {
                bucket: bucket.bucket().to_string(),
                settings: file_store::Settings {
                    region: Some(S3_REGION.to_string()),
                    endpoint: Some(bucket.endpoint().to_string()),
                    access_key_id: Some(S3_CREDENTIAL.to_string()),
                    secret_access_key: Some(S3_CREDENTIAL.to_string()),
                },
            },
            start_after,
            poll_duration: Some(self.poll_duration),
            offset: None,
            idle_timeout: Some(self.idle_timeout),
        }
    }

    /// Output settings writing to this harness's catalog.
    pub fn output_settings(&self) -> OutputSettings {
        OutputSettings {
            catalog: self.iceberg.as_settings(),
            max_batch_rows: 10_000,
            batch_timeout: self.batch_timeout,
        }
    }

    /// State dir for `route`, stable for the life of the harness so a route
    /// can be restarted on the same state.
    pub fn state_dir(&self, route: &str) -> PathBuf {
        self.state_root.path().join(route)
    }

    pub fn route_settings(&self, route: &str) -> RouteSettings {
        RouteSettings::new(self.state_dir(route))
    }

    /// Delete the buckets (required: `AwsLocal` cannot clean up on drop).
    pub async fn cleanup(self) -> anyhow::Result<()> {
        for bucket in self.buckets {
            bucket.cleanup().await?;
        }
        Ok(())
    }
}
