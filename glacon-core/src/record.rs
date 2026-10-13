/// The value a route carries from its inputs, through its middleware, to its
/// outputs. Every input of a route produces the same record type.
///
/// Implemented for every `Send + Sync + 'static` type.
pub trait GlaconRecord: Send + Sync + 'static {}

impl<T: Send + Sync + 'static> GlaconRecord for T {}
