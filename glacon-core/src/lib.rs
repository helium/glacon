//! Route records from inputs, through a middleware, to outputs.
//!
//! A [`Route`] reads one record type from N inputs, runs every batch through
//! a [`GlaconCoreMiddleware`] that picks which of its M outputs get each
//! record, and writes them. A route is an oracles `ManagedTask`: run it in
//! your app's `TaskManager`, alongside your other routes and tasks.
//!
//! This crate knows nothing of where records come from or go to. You
//! provide, each with `init` / main hook / `terminate`:
//! - a [`GlaconCoreInput`] per input: produces [`Batch`]es of records;
//! - optionally a [`GlaconCoreMiddleware`]: record → which outputs (default:
//!   every output);
//! - a [`GlaconCoreOutput`] per output: writes the records routed to it.

mod error;
mod input;
mod lifecycle;
pub mod metrics;
mod output;
mod processor;
mod record;
mod route;
mod task;

pub use error::{BoxError, Error, Result};
pub use input::{Batch, GlaconCoreInput, InputContext};
pub use output::{GlaconCoreOutput, OutputContext};
pub use processor::middleware::{
    AllOutputs, Destination, GlaconCoreMiddleware, MAX_OUTPUTS, MiddlewareContext, OutputId,
    OutputIndex, OutputSet,
};
pub use record::GlaconRecord;
pub use route::{Route, RouteBuilder};

// Re-exported so inputs, outputs and middlewares can be implemented, and
// routes run, without matching versions.
pub use async_trait::async_trait;
pub use task_manager;
pub use triggered;
