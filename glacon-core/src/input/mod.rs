//! Inputs: where a route's records come from, one batch at a time. Each
//! input runs in its own task and feeds its own processor.

mod batch;
mod context;
mod task;
mod r#trait;

pub use batch::Batch;
pub use context::InputContext;
pub use r#trait::{GlaconCoreInput, GlaconCoreInputInit, InitializedInput};

pub(crate) use task::{PendingInput, RouteInput, run};
