//! Route protobuf files from S3 buckets into Iceberg tables, on
//! [`glacon_core`].
//!
//! glacon-core runs routes over any input and output; this crate provides:
//! - [`FileStoreInput`]: polls one bucket with `file_store`; a
//!   [`GlaconInput`] decodes its protobuf messages into records. The route's
//!   ledger is the poller's state;
//! - [`IcebergOutput`]: writes one Iceberg table with `helium_iceberg`; a
//!   [`GlaconOutput`] makes its rows from records.
//!
//! Everything from glacon-core is re-exported, so apps depend on this crate
//! alone.

mod input;
pub mod metrics;
mod output;

pub use glacon_core::*;
pub use input::{FileMeta, FileStoreInput, GlaconInput, InputSettings};
pub use output::{GlaconOutput, IcebergOutput, OutputSettings};

// Re-exported so apps can name these types without matching versions.
pub use file_store;
pub use helium_iceberg;
