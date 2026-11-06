use std::sync::Arc;

/// Handed to [`GlaconCoreMiddleware::init`](super::GlaconCoreMiddleware::init).
#[derive(Debug, Clone)]
pub struct MiddlewareContext {
    pub(crate) route: Arc<str>,
    pub(crate) input: Arc<str>,
}

impl MiddlewareContext {
    pub fn route(&self) -> &str {
        &self.route
    }

    /// The input whose processor runs this middleware.
    pub fn input(&self) -> &str {
        &self.input
    }
}
