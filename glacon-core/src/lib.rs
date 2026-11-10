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
//! - a [`GlaconCoreInput`] per input, producing [`Batch`]es of records;
//! - optionally a [`GlaconCoreMiddleware`]: the records of a batch → one
//!   [`RecordResult`] per record, the record the outputs get (possibly of
//!   another type) or why it failed, in the middleware's own
//!   [`GlaconRecordError`] type (default: [`Passthrough`], which never
//!   fails);
//! - a [`GlaconCoreOutput`] per output, writing the records it keeps and
//!   handling the failures as it wants.
//!
//! Inputs and outputs are their configuration: the route calls their `init`
//! once, which builds the `State` every later call gets.
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

pub use error::{BoxError, Error, ErrorChain, Result};
pub use input::{Batch, BatchId, GlaconCoreInput, InitializedInput, InputContext};
pub use ledger::{InputLedger, Ledger, STATE_DB};
pub use output::{GlaconCoreOutput, InitializedOutput, OutputContext};
pub use processor::middleware::{GlaconCoreMiddleware, MiddlewareContext, Passthrough};
pub use record::{GlaconRecord, GlaconRecordError, RecordResult};
pub use route::{FreshState, Route, RouteBuilder, RouteOutputs, RouteSettings};

// Re-exported so inputs, outputs and middlewares can be implemented, and
// routes run, without matching versions.
pub use async_trait::async_trait;
pub use task_manager;
pub use triggered;
