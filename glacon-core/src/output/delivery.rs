use std::sync::Arc;

/// The records of one batch an output gets: the batch is shared by every
/// output, `picks` are the positions routed to this one.
pub(crate) struct Delivery<R> {
    pub records: Arc<Vec<R>>,
    pub picks: Vec<usize>,
}
