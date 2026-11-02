//! Everything a file-store input needs: polling a bucket with oracles'
//! `file_store` and decoding its protobuf files; the poller's state is the
//! route's ledger.

mod file_meta;
mod input;
mod settings;
mod state;
mod r#trait;

pub use file_meta::FileMeta;
pub use input::{FileStoreInput, FileStoreState};
pub use settings::InputSettings;
pub use r#trait::GlaconInput;
