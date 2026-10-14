use crate::ledger::InputLedger;
use std::sync::Arc;

/// Handed to [`GlaconCoreInputInit::init`](super::GlaconCoreInputInit::init).
#[derive(Debug, Clone)]
pub struct InputContext {
    pub(crate) route: Arc<str>,
    pub(crate) input: Arc<str>,
    pub(crate) ledger: InputLedger,
}

impl InputContext {
    pub fn route(&self) -> &str {
        &self.route
    }

    /// The input's name, stable across runs.
    pub fn input(&self) -> &str {
        &self.input
    }

    /// What this input already delivered: where to resume reading, and which
    /// batches are done.
    pub fn ledger(&self) -> &InputLedger {
        &self.ledger
    }
}
