use std::sync::Arc;

/// Handed to [`GlaconCoreOutputInit::init`](super::GlaconCoreOutputInit::init).
#[derive(Debug, Clone)]
pub struct OutputContext {
    pub(crate) route: Arc<str>,
    pub(crate) output: Arc<str>,
}

impl OutputContext {
    pub fn route(&self) -> &str {
        &self.route
    }

    /// The output's name, stable across runs.
    pub fn output(&self) -> &str {
        &self.output
    }
}
