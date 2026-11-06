//! Outputs: where records are written, each keeping what it wants. Each
//! output runs in its own task and is fed by every processor.

mod context;
mod delivery;
mod task;
mod r#trait;

pub use context::OutputContext;
pub use r#trait::{GlaconCoreOutput, InitializedOutput};

pub(crate) use delivery::{BatchTicket, Delivery};
pub(crate) use task::{InitOutput, PendingOutput, RouteOutput, run};
