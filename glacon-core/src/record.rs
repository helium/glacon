/// The value a route carries from its inputs, through its middleware, to its
/// outputs. Every input of a route produces the same record type.
///
/// Implemented for every `Send + Sync + 'static` type.
pub trait GlaconRecord: Send + Sync + 'static {}

impl<T: Send + Sync + 'static> GlaconRecord for T {}

/// What the middleware made of one input record: `Ok` with the record the
/// outputs get, or `Err` with why it could not make one. Every output gets
/// both, and decides what to do with each (write the errors to a rejects
/// table, count them, ignore them).
pub type RecordResult<R> = Result<R, String>;
