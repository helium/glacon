//! Running outputs: each stages rows for the file being processed and hands
//! them to its table's batched writer. Outputs are type-erased so a route can
//! hold outputs with different row types.

mod batched_sink;
mod connected_output;
mod entry;
mod sink;
mod spec;

pub(crate) use connected_output::ConnectedOutput;
pub(crate) use entry::OutputEntry;
pub(crate) use sink::OutputSink;
pub(crate) use spec::OutputSpec;
