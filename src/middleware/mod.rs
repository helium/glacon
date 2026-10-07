//! Picks which outputs of a route receive each record.

mod all_outputs;
mod destination;
mod output_id;
mod output_index;
mod output_set;
mod r#trait;

pub use all_outputs::AllOutputs;
pub use destination::Destination;
pub use output_id::OutputId;
pub use output_index::OutputIndex;
pub use output_set::{MAX_OUTPUTS, OutputSet};
pub use r#trait::GlaconMiddleware;
