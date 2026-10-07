/// Position of an output within its route, looked up by name with
/// [`OutputIndex::id`](crate::OutputIndex::id).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OutputId(pub(crate) u8);
