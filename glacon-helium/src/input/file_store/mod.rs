//! Everything a file-store input needs: polling a bucket with oracles'
//! `file_store`, decoding its protobuf files, and remembering what was read.

mod file_meta;
mod input;
mod settings;
mod state;
mod r#trait;

pub use file_meta::FileMeta;
pub use input::FileStoreInput;
pub use settings::InputSettings;
pub use state::{MemoryState, PollerState};
pub use r#trait::GlaconInput;
