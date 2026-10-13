//! Everything an Iceberg output needs: creating the table, turning records
//! into rows, and writing them through `helium_iceberg`'s batched writer.

mod output;
mod settings;
mod r#trait;

pub use output::IcebergOutput;
pub use settings::OutputSettings;
pub use r#trait::GlaconOutput;
