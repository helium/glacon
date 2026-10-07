//! Per-route SQLite state: processed files, in-flight deliveries and dedup
//! keys. It is the route's only source of truth.

use crate::Result;
use chrono::{DateTime, Utc};
use sqlx::{
    QueryBuilder, Sqlite, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{
    collections::HashSet,
    path::Path,
    sync::{
        LazyLock,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};
use twox_hash::XxHash3_128;

/// File name of the route database inside its state dir.
pub const STATE_DB: &str = "state.db";

const INITIALIZED_AT: &str = "initialized_at";

/// SQLite caps bound parameters per statement; stay well below the historical
/// 999 limit. Lookups bind one parameter per hash, upserts two.
const SELECT_CHUNK: usize = 900;
const UPSERT_CHUNK: usize = 450;

/// Minimum interval between two prunes of `dedup_keys`.
static PRUNE_INTERVAL: LazyLock<Duration> =
    LazyLock::new(|| humantime::parse_duration("1 hour").expect("valid duration literal"));

/// 128-bit hash of a record's dedup key.
pub type KeyHash = [u8; 16];

pub fn hash_key(key: &str) -> KeyHash {
    XxHash3_128::oneshot(key.as_bytes()).to_be_bytes()
}

pub struct StateDb {
    pool: SqlitePool,
    dedup_retention: Duration,
    /// Wall-clock ms of the last `dedup_keys` prune.
    last_dedup_prune_ms: AtomicI64,
}

impl StateDb {
    /// Open (creating if needed) `<dir>/state.db` and apply migrations.
    pub async fn open(dir: &Path, dedup_retention: Duration) -> Result<Self> {
        tokio::fs::create_dir_all(dir).await?;

        let options = SqliteConnectOptions::new()
            .filename(dir.join(STATE_DB))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(humantime::parse_duration("30s")?);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;

        Self::from_pool(pool, dedup_retention).await
    }

    async fn from_pool(pool: SqlitePool, dedup_retention: Duration) -> Result<Self> {
        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self {
            pool,
            dedup_retention,
            last_dedup_prune_ms: AtomicI64::new(0),
        })
    }

    /// Whether this database has never been initialized (no route has started
    /// on it yet). Drives the fresh-state guard.
    pub async fn is_fresh(&self) -> Result<bool> {
        let marker: Option<String> = sqlx::query_scalar("SELECT value FROM meta WHERE key = ?")
            .bind(INITIALIZED_AT)
            .fetch_optional(&self.pool)
            .await?;

        Ok(marker.is_none())
    }

    pub async fn mark_initialized(&self) -> Result {
        sqlx::query("INSERT OR IGNORE INTO meta (key, value) VALUES (?, ?)")
            .bind(INITIALIZED_AT)
            .bind(Utc::now().to_rfc3339())
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    // ---- processed files (poller state) ----

    /// Timestamp of the newest processed file of `input`.
    pub async fn latest_file_timestamp(&self, input: &str) -> Result<Option<DateTime<Utc>>> {
        let ms: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(file_timestamp_ms) FROM processed_files WHERE input = ?",
        )
        .bind(input)
        .fetch_one(&self.pool)
        .await?;

        Ok(ms.and_then(DateTime::from_timestamp_millis))
    }

    pub async fn is_file_processed(&self, input: &str, file_key: &str) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM processed_files WHERE input = ? AND file_key = ?)",
        )
        .bind(input)
        .bind(file_key)
        .fetch_one(&self.pool)
        .await?)
    }

    /// Delete processed-file rows of `input` older than `before`. The newest
    /// row is always kept, since it is the input's resume point.
    pub async fn clean_processed_files(&self, input: &str, before: DateTime<Utc>) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM processed_files \
             WHERE input = ? AND file_timestamp_ms < ? \
               AND file_timestamp_ms < (SELECT MAX(file_timestamp_ms) FROM processed_files WHERE input = ?)",
        )
        .bind(input)
        .bind(before.timestamp_millis())
        .bind(input)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    // ---- in-flight deliveries ----

    /// Outputs that already durably took `file_key` (non-empty only when a
    /// previous attempt at the file was interrupted).
    pub async fn in_flight_deliveries(
        &self,
        input: &str,
        file_key: &str,
    ) -> Result<HashSet<String>> {
        let outputs: Vec<String> = sqlx::query_scalar(
            "SELECT output FROM in_flight_deliveries WHERE input = ? AND file_key = ?",
        )
        .bind(input)
        .bind(file_key)
        .fetch_all(&self.pool)
        .await?;

        Ok(outputs.into_iter().collect())
    }

    pub async fn record_in_flight_delivery(
        &self,
        input: &str,
        file_key: &str,
        output: &str,
    ) -> Result {
        sqlx::query(
            "INSERT OR IGNORE INTO in_flight_deliveries (input, file_key, output) VALUES (?, ?, ?)",
        )
        .bind(input)
        .bind(file_key)
        .bind(output)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ---- dedup keys ----

    /// The subset of `hashes` already known.
    pub async fn known_keys(&self, hashes: &[KeyHash]) -> Result<HashSet<KeyHash>> {
        let mut found = HashSet::new();
        for chunk in hashes.chunks(SELECT_CHUNK) {
            let mut qb: QueryBuilder<Sqlite> =
                QueryBuilder::new("SELECT key_hash FROM dedup_keys WHERE key_hash IN (");
            let mut separated = qb.separated(", ");

            for hash in chunk {
                separated.push_bind(hash.to_vec());
            }
            qb.push(")");

            let rows: Vec<Vec<u8>> = qb.build_query_scalar().fetch_all(&self.pool).await?;

            found.extend(
                rows.into_iter()
                    .filter_map(|row| KeyHash::try_from(row.as_slice()).ok()),
            );
        }
        Ok(found)
    }

    /// Drop `dedup_keys` older than the dedup retention, at most once per hour.
    pub async fn prune_dedup_keys_if_due(&self, now: DateTime<Utc>) -> Result<u64> {
        let now_ms = now.timestamp_millis();
        let last = self.last_dedup_prune_ms.load(Ordering::Relaxed);

        if now_ms - last < PRUNE_INTERVAL.as_millis() as i64 {
            return Ok(0);
        }
        self.last_dedup_prune_ms.store(now_ms, Ordering::Relaxed);

        let cutoff = now_ms - self.dedup_retention.as_millis() as i64;
        let result = sqlx::query("DELETE FROM dedup_keys WHERE last_seen_ms < ?")
            .bind(cutoff)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected())
    }

    // ---- file completion ----

    /// Atomically mark a file processed: record it, remember (or refresh) the
    /// dedup keys it carried, and clear its delivery ledger.
    pub async fn complete_file(
        &self,
        input: &str,
        file_key: &str,
        file_timestamp: DateTime<Utc>,
        key_hashes: &[KeyHash],
        now: DateTime<Utc>,
    ) -> Result {
        let now_ms = now.timestamp_millis();
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "INSERT OR REPLACE INTO processed_files (input, file_key, file_timestamp_ms, processed_at_ms) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(input)
        .bind(file_key)
        .bind(file_timestamp.timestamp_millis())
        .bind(now_ms)
        .execute(&mut *tx)
        .await?;

        for chunk in key_hashes.chunks(UPSERT_CHUNK) {
            let mut qb: QueryBuilder<Sqlite> =
                QueryBuilder::new("INSERT INTO dedup_keys (key_hash, last_seen_ms) ");
            qb.push_values(chunk, |mut b, hash| {
                b.push_bind(hash.to_vec()).push_bind(now_ms);
            });
            qb.push(" ON CONFLICT (key_hash) DO UPDATE SET last_seen_ms = excluded.last_seen_ms");
            qb.build().execute(&mut *tx).await?;
        }

        sqlx::query("DELETE FROM in_flight_deliveries WHERE input = ? AND file_key = ?")
            .bind(input)
            .bind(file_key)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::Context;

    async fn db(pool: SqlitePool) -> anyhow::Result<StateDb> {
        Ok(StateDb::from_pool(pool, humantime::parse_duration("10s")?).await?)
    }

    fn ts(ms: i64) -> anyhow::Result<DateTime<Utc>> {
        DateTime::from_timestamp_millis(ms).context("timestamp out of range")
    }

    #[sqlx::test(migrations = false)]
    async fn fresh_until_marked(pool: SqlitePool) -> anyhow::Result<()> {
        let db = db(pool).await?;
        assert!(db.is_fresh().await?);
        db.mark_initialized().await?;
        db.mark_initialized().await?;
        assert!(!db.is_fresh().await?);
        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn processed_files_are_tracked_per_input(pool: SqlitePool) -> anyhow::Result<()> {
        let db = db(pool).await?;
        assert_eq!(db.latest_file_timestamp("a").await?, None);

        db.complete_file("a", "f.1", ts(1_000)?, &[], ts(5)?)
            .await?;
        db.complete_file("a", "f.3", ts(3_000)?, &[], ts(5)?)
            .await?;
        db.complete_file("b", "f.9", ts(9_000)?, &[], ts(5)?)
            .await?;

        assert_eq!(db.latest_file_timestamp("a").await?, Some(ts(3_000)?));
        assert!(db.is_file_processed("a", "f.1").await?);
        assert!(!db.is_file_processed("b", "f.1").await?);
        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn clean_keeps_the_newest_file(pool: SqlitePool) -> anyhow::Result<()> {
        let db = db(pool).await?;
        for (key, ms) in [("f.1", 1_000), ("f.2", 2_000), ("f.3", 3_000)] {
            db.complete_file("a", key, ts(ms)?, &[], ts(0)?).await?;
        }

        // Cut-off after every file: all but the newest go.
        assert_eq!(db.clean_processed_files("a", ts(10_000)?).await?, 2);
        assert_eq!(db.latest_file_timestamp("a").await?, Some(ts(3_000)?));
        assert!(db.is_file_processed("a", "f.3").await?);
        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn in_flight_deliveries_are_cleared_when_the_file_completes(
        pool: SqlitePool,
    ) -> anyhow::Result<()> {
        let db = db(pool).await?;
        db.record_in_flight_delivery("a", "f.1", "lake").await?;
        db.record_in_flight_delivery("a", "f.1", "lake").await?;
        db.record_in_flight_delivery("a", "f.1", "archive").await?;
        assert_eq!(db.in_flight_deliveries("a", "f.1").await?.len(), 2);
        assert!(db.in_flight_deliveries("b", "f.1").await?.is_empty());

        db.complete_file("a", "f.1", ts(1)?, &[], ts(1)?).await?;
        assert!(db.in_flight_deliveries("a", "f.1").await?.is_empty());
        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn dedup_keys_are_recorded_and_pruned(pool: SqlitePool) -> anyhow::Result<()> {
        let db = db(pool).await?;
        let base = 1_700_000_000_000;
        let (old, new) = (hash_key("old"), hash_key("new"));

        db.complete_file("a", "f.1", ts(1)?, &[old], ts(base)?)
            .await?;
        db.complete_file("a", "f.2", ts(2)?, &[new], ts(base + 8_000)?)
            .await?;
        assert_eq!(db.known_keys(&[old, new, hash_key("x")]).await?.len(), 2);

        // Retention 10s: at base+15s only "old" is past it.
        assert_eq!(db.prune_dedup_keys_if_due(ts(base + 15_000)?).await?, 1);
        assert_eq!(db.known_keys(&[old, new]).await?, HashSet::from([new]));

        // Throttled within the hour.
        assert_eq!(db.prune_dedup_keys_if_due(ts(base + 60_000)?).await?, 0);
        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn seeing_a_key_again_refreshes_it(pool: SqlitePool) -> anyhow::Result<()> {
        let db = db(pool).await?;
        let base = 1_700_000_000_000;
        let id = hash_key("k");

        db.complete_file("a", "f.1", ts(1)?, &[id], ts(base)?)
            .await?;
        db.complete_file("b", "f.2", ts(2)?, &[id], ts(base + 8_000)?)
            .await?;

        assert_eq!(db.prune_dedup_keys_if_due(ts(base + 15_000)?).await?, 0);
        assert!(db.known_keys(&[id]).await?.contains(&id));
        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn large_batches_are_chunked(pool: SqlitePool) -> anyhow::Result<()> {
        let db = db(pool).await?;
        let ids: Vec<KeyHash> = (0..2_000).map(|i| hash_key(&i.to_string())).collect();
        db.complete_file("a", "f.1", ts(1)?, &ids, ts(1)?).await?;
        assert_eq!(db.known_keys(&ids).await?.len(), 2_000);
        Ok(())
    }
}
