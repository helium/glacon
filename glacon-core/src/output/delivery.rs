use crate::{BatchId, Result, ledger::InputLedger};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// One batch handed to an output: its records, shared by every output.
pub(crate) struct Delivery<R> {
    pub ticket: Arc<BatchTicket>,
    pub records: Arc<Vec<R>>,
}

/// One batch on its way to its outputs, shared by their deliveries. Each
/// output records that it took the batch, then counts down; the last one
/// marks the batch done.
pub(crate) struct BatchTicket {
    pub id: BatchId,
    /// Read again after a crash: outputs are asked `already_written` first.
    pub replayed: bool,
    ledger: InputLedger,
    /// Outputs that still have to take the batch.
    remaining: AtomicUsize,
}

impl BatchTicket {
    pub fn new(id: BatchId, replayed: bool, ledger: InputLedger, outputs: usize) -> Self {
        Self {
            id,
            replayed,
            ledger,
            remaining: AtomicUsize::new(outputs),
        }
    }

    /// `output` durably took the batch. Recorded before counting down, so no
    /// delivery row lands after the batch is done.
    pub async fn delivered(&self, output: &str) -> Result {
        self.ledger.ledger().deliver(&self.id, output).await?;
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.ledger.complete(&self.id).await?;
        }
        Ok(())
    }
}
