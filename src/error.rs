use thiserror::Error;

pub type Result<T = (), E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    /// The route definition is invalid (caught when building it).
    #[error("invalid route {route}: {reason}")]
    InvalidRoute { route: String, reason: String },

    /// An output table already has data but the route has no state yet.
    #[error(
        "route {route}: output {output} table {table} already has data but the route's \
         state is empty; refusing to start (see FreshState)"
    )]
    FreshStateRefused {
        route: String,
        output: String,
        table: String,
    },

    #[error("state db: {0}")]
    State(#[from] sqlx::Error),

    #[error("state db migration: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("file store: {0}")]
    FileStore(#[from] file_store::Error),

    #[error("iceberg: {0}")]
    Iceberg(#[from] helium_iceberg::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid duration: {0}")]
    Duration(#[from] humantime::DurationError),

    #[error("route {route}: {source}")]
    Task {
        route: String,
        #[source]
        source: task_manager::TaskError,
    },
}

impl Error {
    pub(crate) fn invalid_route(route: &str, reason: impl Into<String>) -> Self {
        Self::InvalidRoute {
            route: route.to_string(),
            reason: reason.into(),
        }
    }
}
