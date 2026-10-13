use super::OutputSet;

/// Where a record goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Destination {
    /// Every output of the route (the default).
    All,
    /// Only these outputs. An empty set behaves like [`Destination::Drop`].
    Only(OutputSet),
    /// No output: the record is discarded.
    Drop,
}
