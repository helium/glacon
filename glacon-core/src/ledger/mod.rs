//! The delivery ledger: `<state_dir>/state.db`, one SQLite database per
//! route. It records which batches each input read, which outputs already
//! have each batch, and when a batch is done. It stores no records: after a
//! crash, batches not done are read again from their source and delivered
//! only to the outputs that don't have them yet.

mod input;

pub use input::InputLedger;

use crate::{BatchId, Result};
use chrono::{DateTime, Utc};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{collections::HashSet, path::Path, time::Duration};

/// File name of the ledger inside a route's state dir.
pub const STATE_DB: &str = "state.db";

const INITIALIZED_AT: &str = "initialized_at";
const IN_FLIGHT: &str = "in_flight";
const DONE: &str = "done";

/// What reading a batch found in the ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Read {
    /// Never seen: now in flight.
    First,
    /// Read before and not done: delivered again to the outputs that don't
    /// have it.
    Replay,
    /// Already done: skip it.
    Done,
}

pub struct Ledger {
    pool: SqlitePool,
}

impl Ledger {
    /// Open (creating if needed) `<state_dir>/state.db` and apply migrations.
    pub async fn open(state_dir: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(state_dir).await?;
        let options = SqliteConnectOptions::new()
            .filename(state_dir.join(STATE_DB))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(30));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self { pool })
    }

    /// Whether no route ever started on this ledger. Drives the fresh-state
    /// guard.
    pub async fn is_fresh(&self) -> Result<bool> {
        let marker: Option<String> = sqlx::query_scalar("SELECT value FROM meta WHERE key = ?")
            .bind(INITIALIZED_AT)
            .fetch_optional(&self.pool)
            .await?;
        Ok(marker.is_none())
    }

    pub(crate) async fn mark_initialized(&self) -> Result {
        sqlx::query("INSERT OR IGNORE INTO meta (key, value) VALUES (?, ?)")
            .bind(INITIALIZED_AT)
            .bind(Utc::now().to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Record that `batch` was read: in flight unless already known. Tells
    /// whether it is new, a replay, or already done.
    pub(crate) async fn read(&self, batch: &BatchId, now: DateTime<Utc>) -> Result<Read> {
        let inserted = sqlx::query(
            "INSERT INTO batches (input, batch_key, batch_ts_ms, status, read_at_ms) \
             VALUES (?, ?, ?, ?, ?) ON CONFLICT (input, batch_key) DO NOTHING",
        )
        .bind(&*batch.input)
        .bind(&batch.key)
        .bind(batch.timestamp.timestamp_millis())
        .bind(IN_FLIGHT)
        .bind(now.timestamp_millis())
        .execute(&self.pool)
        .await?
        .rows_affected();
        if inserted == 1 {
            return Ok(Read::First);
        }
        Ok(if self.is_done(&batch.input, &batch.key).await? {
            Read::Done
        } else {
            Read::Replay
        })
    }

    /// Whether `input`'s batch `key` is done: every output has it.
    pub async fn is_done(&self, input: &str, key: &str) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM batches WHERE input = ? AND batch_key = ? AND status = ?)",
        )
        .bind(input)
        .bind(key)
        .bind(DONE)
        .fetch_one(&self.pool)
        .await?)
    }

    /// The outputs that already durably took an in-flight batch.
    pub async fn deliveries(&self, input: &str, key: &str) -> Result<HashSet<String>> {
        let outputs: Vec<String> =
            sqlx::query_scalar("SELECT output FROM deliveries WHERE input = ? AND batch_key = ?")
                .bind(input)
                .bind(key)
                .fetch_all(&self.pool)
                .await?;
        Ok(outputs.into_iter().collect())
    }

    /// Record that `output` durably took `batch`.
    pub(crate) async fn deliver(&self, batch: &BatchId, output: &str) -> Result {
        sqlx::query("INSERT OR IGNORE INTO deliveries (input, batch_key, output) VALUES (?, ?, ?)")
            .bind(&*batch.input)
            .bind(&batch.key)
            .bind(output)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Mark `batch` done and forget its deliveries, in one transaction.
    pub(crate) async fn complete(&self, batch: &BatchId, now: DateTime<Utc>) -> Result {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE batches SET status = ?, done_at_ms = ? WHERE input = ? AND batch_key = ?",
        )
        .bind(DONE)
        .bind(now.timestamp_millis())
        .bind(&*batch.input)
        .bind(&batch.key)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM deliveries WHERE input = ? AND batch_key = ?")
            .bind(&*batch.input)
            .bind(&batch.key)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Where `input` resumes: its oldest in-flight batch if any (batches
    /// finish out of order), else its newest done one. `None` when it has
    /// none.
    pub async fn resume_point(&self, input: &str) -> Result<Option<DateTime<Utc>>> {
        let oldest_in_flight: Option<i64> = sqlx::query_scalar(
            "SELECT MIN(batch_ts_ms) FROM batches WHERE input = ? AND status = ?",
        )
        .bind(input)
        .bind(IN_FLIGHT)
        .fetch_one(&self.pool)
        .await?;
        let ms = match oldest_in_flight {
            Some(ms) => Some(ms),
            None => {
                sqlx::query_scalar(
                    "SELECT MAX(batch_ts_ms) FROM batches WHERE input = ? AND status = ?",
                )
                .bind(input)
                .bind(DONE)
                .fetch_one(&self.pool)
                .await?
            }
        };
        Ok(ms.and_then(DateTime::from_timestamp_millis))
    }

    /// Delete `input`'s done batches older than `before`, keeping its newest
    /// done one (a resume point) and every in-flight one.
    pub(crate) async fn prune(&self, input: &str, before: DateTime<Utc>) -> Result<u64> {
        let pruned = sqlx::query(
            "DELETE FROM batches WHERE input = ? AND status = ? AND batch_ts_ms < ? \
               AND batch_ts_ms < (SELECT MAX(batch_ts_ms) FROM batches WHERE input = ? AND status = ?)",
        )
        .bind(input)
        .bind(DONE)
        .bind(before.timestamp_millis())
        .bind(input)
        .bind(DONE)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(pruned)
    }

    /// How many of `input`'s batches are in flight.
    pub async fn in_flight(&self, input: &str) -> Result<u64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM batches WHERE input = ? AND status = ?")
                .bind(input)
                .bind(IN_FLIGHT)
                .fetch_one(&self.pool)
                .await?;
        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;

    fn ts(ms: i64) -> anyhow::Result<DateTime<Utc>> {
        DateTime::from_timestamp_millis(ms).context("timestamp out of range")
    }

    fn batch(input: &str, key: &str, ms: i64) -> anyhow::Result<BatchId> {
        Ok(BatchId {
            input: input.into(),
            key: key.to_string(),
            timestamp: ts(ms)?,
        })
    }

    #[tokio::test]
    async fn fresh_until_marked() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let ledger = Ledger::open(dir.path()).await?;
        assert!(ledger.is_fresh().await?);
        ledger.mark_initialized().await?;
        ledger.mark_initialized().await?;
        assert!(!ledger.is_fresh().await?);
        // Kept across opens.
        assert!(!Ledger::open(dir.path()).await?.is_fresh().await?);
        Ok(())
    }

    #[tokio::test]
    async fn reading_tells_new_replayed_and_done_batches() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let ledger = Ledger::open(dir.path()).await?;
        let b = batch("a", "f.1", 1_000)?;

        assert_eq!(ledger.read(&b, ts(5)?).await?, Read::First);
        assert_eq!(ledger.read(&b, ts(6)?).await?, Read::Replay);
        assert!(!ledger.is_done("a", "f.1").await?);

        ledger.complete(&b, ts(7)?).await?;
        assert_eq!(ledger.read(&b, ts(8)?).await?, Read::Done);
        assert!(ledger.is_done("a", "f.1").await?);
        assert!(!ledger.is_done("b", "f.1").await?, "per input");
        Ok(())
    }

    #[tokio::test]
    async fn completing_forgets_deliveries() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let ledger = Ledger::open(dir.path()).await?;
        let b = batch("a", "f.1", 1_000)?;
        ledger.read(&b, ts(5)?).await?;

        ledger.deliver(&b, "lake").await?;
        ledger.deliver(&b, "lake").await?;
        ledger.deliver(&b, "archive").await?;
        assert_eq!(
            ledger.deliveries("a", "f.1").await?,
            HashSet::from(["lake".to_string(), "archive".to_string()])
        );

        ledger.complete(&b, ts(6)?).await?;
        assert!(ledger.deliveries("a", "f.1").await?.is_empty());
        assert_eq!(ledger.in_flight("a").await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn resumes_at_the_oldest_in_flight_batch() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let ledger = Ledger::open(dir.path()).await?;
        assert_eq!(ledger.resume_point("a").await?, None);

        let (b1, b2, b3) = (
            batch("a", "f.1", 1_000)?,
            batch("a", "f.2", 2_000)?,
            batch("a", "f.3", 3_000)?,
        );
        for b in [&b1, &b2, &b3] {
            ledger.read(b, ts(5)?).await?;
        }
        // Out of order: the later batches finish first.
        ledger.complete(&b2, ts(6)?).await?;
        ledger.complete(&b3, ts(6)?).await?;
        assert_eq!(ledger.resume_point("a").await?, Some(ts(1_000)?));

        ledger.complete(&b1, ts(7)?).await?;
        assert_eq!(ledger.resume_point("a").await?, Some(ts(3_000)?));
        assert_eq!(ledger.resume_point("b").await?, None, "per input");
        Ok(())
    }

    #[tokio::test]
    async fn pruning_keeps_the_newest_done_and_every_in_flight_batch() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let ledger = Ledger::open(dir.path()).await?;
        for (key, ms) in [("f.1", 1_000), ("f.2", 2_000), ("f.3", 3_000)] {
            let b = batch("a", key, ms)?;
            ledger.read(&b, ts(0)?).await?;
            ledger.complete(&b, ts(0)?).await?;
        }
        let old_in_flight = batch("a", "f.0", 500)?;
        ledger.read(&old_in_flight, ts(0)?).await?;

        // Cut-off after every batch: only f.1 and f.2 go.
        assert_eq!(ledger.prune("a", ts(10_000)?).await?, 2);
        assert!(ledger.is_done("a", "f.3").await?);
        assert_eq!(ledger.in_flight("a").await?, 1);
        assert_eq!(ledger.resume_point("a").await?, Some(ts(500)?));
        Ok(())
    }
}
