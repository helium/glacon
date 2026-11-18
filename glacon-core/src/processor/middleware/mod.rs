//! Transforms each batch's records before they reach the outputs.

mod context;
mod passthrough;
mod r#trait;

pub use context::MiddlewareContext;
pub use passthrough::Passthrough;
pub use r#trait::GlaconCoreMiddleware;
