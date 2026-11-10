use thiserror::Error;

pub type Result<T = (), E = Error> = std::result::Result<T, E>;

/// Error type of inputs, middlewares and outputs: whatever their backend
/// returns.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Error)]
pub enum Error {
    /// The route definition is invalid (caught when building it).
    #[error("invalid route {route}: {reason}")]
    InvalidRoute { route: String, reason: String },

    /// An input failed to initialize, produce a batch or terminate.
    #[error("route {route}: input {input}: {source}")]
    Input {
        route: String,
        input: String,
        #[source]
        source: BoxError,
    },

    /// The middleware of an input's processor failed.
    #[error("route {route}: middleware of input {input}: {source}")]
    Middleware {
        route: String,
        input: String,
        #[source]
        source: BoxError,
    },

    /// An output failed to initialize, write or terminate.
    #[error("route {route}: output {output}: {source}")]
    Output {
        route: String,
        output: String,
        #[source]
        source: BoxError,
    },

    /// One of a route's tasks panicked.
    #[error("route {route}: {task} panicked: {message}")]
    Panic {
        route: String,
        task: String,
        message: String,
    },

    /// An output already has data but the route's ledger is empty.
    #[error(
        "route {route}: output {output} already has data but the route's state is empty; \
         refusing to start (see FreshState)"
    )]
    FreshStateRefused { route: String, output: String },

    #[error("state db: {0}")]
    State(#[from] sqlx::Error),

    #[error("state db migration: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    pub(crate) fn invalid_route(route: &str, reason: impl Into<String>) -> Self {
        Self::InvalidRoute {
            route: route.to_string(),
            reason: reason.into(),
        }
    }

    pub(crate) fn input(route: &str, input: &str, source: impl Into<BoxError>) -> Self {
        Self::Input {
            route: route.to_string(),
            input: input.to_string(),
            source: source.into(),
        }
    }

    pub(crate) fn middleware(route: &str, input: &str, source: impl Into<BoxError>) -> Self {
        Self::Middleware {
            route: route.to_string(),
            input: input.to_string(),
            source: source.into(),
        }
    }

    pub(crate) fn output(route: &str, output: &str, source: impl Into<BoxError>) -> Self {
        Self::Output {
            route: route.to_string(),
            output: output.to_string(),
            source: source.into(),
        }
    }
}

/// An error and every cause under it, `: `-separated, for logs: a plain
/// `%error` shows only the top message, losing the cause (the HTTP or IO
/// error behind a write failure, say).
///
/// A cause already ending its parent's message (as `#[error("...:
/// {source}")]` does) is not repeated.
///
/// ```
/// # use glacon_core::ErrorChain;
/// # let error = std::io::Error::other("disk full");
/// tracing::error!(error = %ErrorChain(&error), "write failed");
/// ```
pub struct ErrorChain<'a>(pub &'a (dyn std::error::Error + 'static));

impl std::fmt::Display for ErrorChain<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut shown = self.0.to_string();
        let mut source = self.0.source();
        while let Some(cause) = source {
            let message = cause.to_string();
            if !shown.ends_with(&message) {
                shown.push_str(": ");
                shown.push_str(&message);
            }
            source = cause.source();
        }
        f.write_str(&shown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Error)]
    #[error("{message}")]
    struct Layer {
        message: &'static str,
        #[source]
        source: Option<Box<Layer>>,
    }

    fn layer(message: &'static str, source: Option<Layer>) -> Layer {
        Layer {
            message,
            source: source.map(Box::new),
        }
    }

    #[test]
    fn chain_shows_every_cause_once() {
        let io = layer("broken pipe", None);
        let http = layer("request failed", Some(io));
        // Its message already ends with its source's.
        let output = Error::output("r", "lake", http);
        assert_eq!(
            ErrorChain(&output).to_string(),
            "route r: output lake: request failed: broken pipe"
        );
        assert_eq!(ErrorChain(&layer("alone", None)).to_string(), "alone");
    }
}
