//! Processors: one per input, each running its own middleware over the
//! input's batches and handing the records to the outputs.

pub mod middleware;
mod task;

pub(crate) use task::{OutputSender, RouteMiddleware, run};
