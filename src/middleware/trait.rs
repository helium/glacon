use super::Destination;

/// Decides which outputs, if any, receive each record.
///
/// A middleware that selects outputs is created with
/// [`RouteBuilder::middleware_with`](crate::RouteBuilder::middleware_with),
/// which hands it the [`OutputId`](super::OutputId)s of those outputs.
pub trait GlaconMiddleware<R>: Send + Sync + 'static {
    fn route(&self, record: &R) -> Destination;
}
