//! Outputs: where the records routed by the middleware are written. Each
//! output runs in its own task and is fed by every processor.

mod context;
mod delivery;
mod task;
mod r#trait;

pub use context::OutputContext;
pub use r#trait::{GlaconCoreOutput, GlaconCoreOutputInit, InitializedOutput};

pub(crate) use delivery::{BatchTicket, Delivery};
pub(crate) use task::{PendingOutput, RouteOutput, run};
