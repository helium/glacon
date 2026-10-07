//! Route protobuf files from S3 buckets into Iceberg tables.
//!
//! A [`Route`] reads one record type from N inputs (buckets), lets one
//! [`GlaconMiddleware`] pick which of its M outputs (tables) get each record, and
//! keeps its progress in its own SQLite database. [`Glacon`] runs every route
//! as a task.
//!
//! You provide three things:
//! - a [`GlaconInput`] per kind of file: protobuf message -> record;
//! - optionally a [`GlaconMiddleware`]: record -> which outputs (default: all);
//! - a [`GlaconOutput`] per table: record -> rows.

mod error;
mod glacon;
mod input;
pub mod metrics;
mod middleware;
mod output;
mod record;
mod route;
pub mod state;
mod tasks;

pub use error::{Error, Result};
pub use glacon::Glacon;
pub use input::{FileMeta, GlaconInput, InputSettings};
pub use middleware::{
    AllOutputs, Destination, GlaconMiddleware, MAX_OUTPUTS, OutputId, OutputIndex, OutputSet,
};
pub use output::{GlaconOutput, OutputSettings};
pub use record::GlaconRecord;
pub use route::{FreshState, Route, RouteBuilder, RouteSettings};

// Re-exported so apps can name these types without matching versions.
pub use file_store;
pub use helium_iceberg;
pub use task_manager;
