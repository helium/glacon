//! Inputs: where a route's records come from, one batch at a time. Each
//! input runs in its own task and feeds its own processor.

mod batch;
mod context;
mod task;
mod r#trait;

pub use batch::{Batch, BatchId};
pub use context::InputContext;
pub use r#trait::{GlaconCoreInput, InitializedInput};

pub(crate) use batch::ReadBatch;
pub(crate) use task::{InitInput, PendingInput, RouteInput, run};
