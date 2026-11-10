use super::{Ledger, Read};
use crate::{BatchId, Result, metrics};
use chrono::{DateTime, Utc};
use std::{sync::Arc, time::Duration};

/// One input's view of its route's ledger, handed to it in its
/// [`InputContext`](crate::InputContext). An input uses it to resume where
/// it left off: from [`InputLedger::resume_point`], skipping what
/// [`InputLedger::is_done`] says.
#[derive(Clone)]
pub struct InputLedger {
    route: Arc<str>,
    input: Arc<str>,
    ledger: Arc<Ledger>,
    retention: Duration,
}

impl std::fmt::Debug for InputLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputLedger")
            .field("route", &self.route)
            .field("input", &self.input)
            .finish_non_exhaustive()
    }
}

impl InputLedger {
    pub(crate) fn new(
        route: Arc<str>,
        input: Arc<str>,
        ledger: Arc<Ledger>,
        retention: Duration,
    ) -> Self {
        Self {
            route,
            input,
            ledger,
            retention,
        }
    }

    /// Where to resume reading: the oldest batch not done yet if any, else
    /// the newest done one. `None` on a first run.
    pub async fn resume_point(&self) -> Result<Option<DateTime<Utc>>> {
        self.ledger.resume_point(&self.input).await
    }

    /// Whether the batch `key` is done: every output has it. A batch read
    /// but not done must be produced again.
    pub async fn is_done(&self, key: &str) -> Result<bool> {
        self.ledger.is_done(&self.input, key).await
    }

    /// Forget done batches older than both `window_start` (the oldest batch
    /// the input may still see again) and the route's retention. The newest
    /// done batch and every in-flight one are kept.
    pub async fn prune(&self, window_start: DateTime<Utc>) -> Result<u64> {
        let before = window_start.min(Utc::now() - self.retention);
        let pruned = self.ledger.prune(&self.input, before).await?;
        metrics::state_pruned(&self.route, &self.input, pruned);
        Ok(pruned)
    }

    /// Prune what is older than the retention alone.
    pub(crate) async fn prune_expired(&self) -> Result<u64> {
        self.prune(Utc::now()).await
    }

    pub(crate) fn batch_id(&self, key: String, timestamp: DateTime<Utc>) -> BatchId {
        BatchId {
            input: self.input.clone(),
            key,
            timestamp,
        }
    }

    pub(crate) async fn read(&self, batch: &BatchId) -> Result<Read> {
        self.ledger.read(batch, Utc::now()).await
    }

    pub(crate) async fn deliveries(
        &self,
        batch: &BatchId,
    ) -> Result<std::collections::HashSet<String>> {
        self.ledger.deliveries(&batch.input, &batch.key).await
    }

    pub(crate) async fn complete(&self, batch: &BatchId) -> Result {
        self.ledger.complete(batch, Utc::now()).await
    }

    pub(crate) async fn in_flight(&self) -> Result<u64> {
        self.ledger.in_flight(&self.input).await
    }

    pub(crate) fn ledger(&self) -> &Arc<Ledger> {
        &self.ledger
    }
}
