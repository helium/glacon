//! Running inputs: each is polled and its files decoded into records. Inputs
//! are type-erased so a route can mix protobuf messages that decode into the
//! same record.

mod decoded_file;
mod entry;
mod noop_recorder;
mod poller_state;
mod spec;

pub(crate) use decoded_file::{DecodedFile, DecodedFiles};
pub(crate) use entry::InputEntry;
pub(crate) use spec::InputSpec;
