//! Route records from inputs, through a middleware, to outputs.
//!
//! A [`Route`] reads one record type from N inputs, runs every batch through
//! a [`GlaconCoreMiddleware`] that transforms its records, and hands them to
//! its M outputs, each keeping what it wants. A route is an oracles
//! `ManagedTask`: run it in your app's `TaskManager`, alongside your other
//! routes and tasks.
//!
//! This crate knows nothing of where records come from or go to. You
//! provide:
//! - a [`GlaconCoreInputInit`] per input, initialized into a
//!   [`GlaconCoreInput`] that produces [`Batch`]es of records;
//! - optionally a [`GlaconCoreMiddleware`]: the records of a batch → the
//!   records the outputs get, possibly of another type, without the ones to
//!   drop (default: [`Passthrough`]);
//! - a [`GlaconCoreOutputInit`] per output, initialized into a
//!   [`GlaconCoreOutput`] that writes the records it keeps.
//!
//! Each route keeps a ledger, `<state_dir>/state.db` (see [`RouteSettings`]),
//! of the batches it read and the outputs that took them: after a crash, a
//! batch not done is read again and delivered only to the outputs that don't
//! have it. Inputs must be able to produce such batches again.

mod error;
mod input;
mod ledger;
mod lifecycle;
pub mod metrics;
mod output;
mod processor;
mod record;
mod route;
mod task;

pub use error::{BoxError, Error, Result};
pub use input::{
    Batch, BatchId, GlaconCoreInput, GlaconCoreInputInit, InitializedInput, InputContext,
};
pub use ledger::{InputLedger, Ledger, STATE_DB};
pub use output::{GlaconCoreOutput, GlaconCoreOutputInit, InitializedOutput, OutputContext};
pub use processor::middleware::{GlaconCoreMiddleware, MiddlewareContext, Passthrough};
pub use record::GlaconRecord;
pub use route::{FreshState, Route, RouteBuilder, RouteOutputs, RouteSettings};

// Re-exported so inputs, outputs and middlewares can be implemented, and
// routes run, without matching versions.
pub use async_trait::async_trait;
pub use task_manager;
pub use triggered;
