use std::convert::Infallible;

/// The value a route carries from its inputs, through its middleware, to its
/// outputs. Every input of a route produces the same record type.
///
/// Implemented for every `Send + Sync + 'static` type.
pub trait GlaconRecord: Send + Sync + 'static {}

impl<T: Send + Sync + 'static> GlaconRecord for T {}

/// What the middleware made of one input record: `Ok` with the record the
/// outputs get, or `Err` with why it could not make one, in the middleware's
/// own error type `E` (its [`GlaconCoreMiddleware::Error`]). Every output
/// gets both, and decides what to do with each (write the errors to a
/// rejects table, count them, ignore them), matching on `E`'s variants.
///
/// [`GlaconCoreMiddleware::Error`]: crate::GlaconCoreMiddleware::Error
pub type RecordResult<R, E> = Result<R, E>;

/// Why the middleware could not make a record: the error type a middleware
/// declares, typically an enum with one variant per kind of failure,
/// carrying what was wrong with the record (its id, the value that failed).
///
/// ```
/// # use glacon_core::GlaconRecordError;
/// #[derive(Debug, thiserror::Error)]
/// enum VerifyError {
///     #[error("{id}: duplicate")]
///     Duplicate { id: u64 },
///     #[error("{id}: took {ms}ms")]
///     TooSlow { id: u64, ms: u32 },
/// }
///
/// impl GlaconRecordError for VerifyError {
///     fn reason(&self) -> &'static str {
///         match self {
///             Self::Duplicate { .. } => "duplicate",
///             Self::TooSlow { .. } => "too_slow",
///         }
///     }
/// }
/// ```
///
/// Its `Display` is logged, never used as a label.
pub trait GlaconRecordError: std::error::Error + Send + Sync + 'static {
    /// A short, fixed category (`"duplicate"`, `"invalid"`, `"malformed"`):
    /// it labels `glacon_middleware_errors_total`, so it must come from a
    /// small set, never from the record.
    fn reason(&self) -> &'static str;
}

/// The error of a middleware that never fails a record, like
/// [`Passthrough`](crate::Passthrough): routes without a middleware hand
/// their outputs `Result<R, Infallible>`.
impl GlaconRecordError for Infallible {
    fn reason(&self) -> &'static str {
        match *self {}
    }
}
