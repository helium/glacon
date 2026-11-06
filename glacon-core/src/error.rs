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
