//! Inputs: S3 buckets whose protobuf files are decoded into route records.

mod file_meta;
mod runtime;
mod settings;
mod r#trait;

pub use file_meta::FileMeta;
pub use settings::InputSettings;
pub use r#trait::GlaconInput;

pub(crate) use runtime::{DecodedFile, DecodedFiles, InputEntry, InputSpec};
