use std::fmt;

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
pub type RecordResult<R> = Result<R, RecordError>;

/// Why the middleware could not make a record.
///
/// `reason` is a short, fixed category (`"duplicate"`, `"invalid"`,
/// `"malformed"`): it labels `glacon_middleware_errors_total`, so it must
/// come from a small set, never from the record. `message` says what was
/// wrong with this record (its id, the value that failed); it is logged and
/// handed to the outputs, never used as a label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordError {
    pub reason: &'static str,
    pub message: String,
}

impl RecordError {
    pub fn new(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }
}

/// The message alone: the reason is a field of its own.
impl fmt::Display for RecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RecordError {}
