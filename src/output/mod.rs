//! Outputs: Iceberg tables that route records are written to.

mod runtime;
mod settings;
mod r#trait;

pub use settings::OutputSettings;
pub use r#trait::GlaconOutput;

pub(crate) use runtime::{OutputEntry, OutputSink, OutputSpec};
