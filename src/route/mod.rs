//! A route: one record type read from N inputs, filtered by one middleware
//! and written to M outputs, with its state in one SQLite database.
//!
//! At runtime a route is a single task owning its own TaskManager:
//!
//! ```text
//! N pollers ──► 1 processor ──► M writers
//! ```
//!
//! Each input file is read once; a slow output stalls the whole route.

mod builder;
mod fresh_state;
mod processor;
mod processor_task;
mod settings;

pub use builder::RouteBuilder;
pub use fresh_state::FreshState;
pub use settings::RouteSettings;

use crate::{
    Error, GlaconMiddleware, GlaconRecord, Result,
    input::InputSpec,
    output::OutputSpec,
    state::StateDb,
    tasks::{self, NamedTask},
};
use futures::stream;
use processor::Processor;
use processor_task::ProcessorTask;
use std::sync::Arc;
use task_manager::{ManagedTask, TaskFuture, TaskManager};

/// A validated route, ready to run as a task (see [`crate::Glacon`]).
pub struct Route<R> {
    name: String,
    settings: RouteSettings,
    inputs: Vec<Box<dyn InputSpec<R>>>,
    middleware: Box<dyn GlaconMiddleware<R>>,
    outputs: Vec<Box<dyn OutputSpec<R>>>,
    fresh_state: FreshState,
}

impl<R: GlaconRecord> Route<R> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn settings(&self) -> &RouteSettings {
        &self.settings
    }

    /// Open the state, connect every output (creating missing tables), apply
    /// the fresh-state guard and wire the route's tasks.
    pub async fn setup(self) -> Result<TaskManager> {
        let Route {
            name,
            settings,
            inputs,
            middleware,
            outputs,
            fresh_state,
        } = self;

        let db = Arc::new(StateDb::open(&settings.state_dir, settings.dedup_retention).await?);
        let fresh = db.is_fresh().await?;
        let spool_dir = settings.state_dir.join("spool");
        let mut sinks = Vec::with_capacity(outputs.len());
        let mut writers = Vec::with_capacity(outputs.len());

        for output in outputs {
            let output_name = output.name().to_string();
            let connected = output.connect(&spool_dir).await?;
            if fresh && connected.had_data {
                match fresh_state {
                    FreshState::Refuse => {
                        return Err(Error::FreshStateRefused {
                            route: name,
                            output: output_name,
                            table: connected.table,
                        });
                    }
                    FreshState::Proceed => tracing::warn!(
                        route = %name,
                        output = %output_name,
                        table = %connected.table,
                        "state is empty but the table has data; proceeding as configured"
                    ),
                }
            }
            sinks.push(connected.sink);
            writers.push(connected.writer);
        }
        db.mark_initialized().await?;

        let mut files = Vec::with_capacity(inputs.len());
        let mut pollers = Vec::with_capacity(inputs.len());

        for input in inputs {
            let (stream, poller) = input
                .start(&name, db.clone(), settings.files_retention)
                .await?;
            files.push(stream);
            pollers.push(poller);
        }

        let processor = ProcessorTask {
            processor: Processor {
                route: Arc::from(name.as_str()),
                db,
                middleware,
                outputs: sinks,
            },
            files: std::sync::Mutex::new(Box::pin(stream::select_all(files))),
        };

        tracing::info!(route = %name, "route started");

        // Tasks stop in reverse order: pollers first, then the processor
        // (finishing its current file), then the writers flush their spools.
        Ok(tasks::task_manager(
            writers
                .into_iter()
                .chain([NamedTask::new("processor", processor)])
                .chain(pollers),
        ))
    }

    async fn run(self, shutdown: triggered::Listener) -> Result {
        let name = self.name.clone();
        let task_manager = self.setup().await?;
        Box::new(task_manager)
            .start_task(shutdown)
            .await
            .map_err(|source| Error::Task {
                route: name.clone(),
                source,
            })?;
        tracing::info!(route = %name, "route stopped");
        Ok(())
    }
}

impl<R: GlaconRecord> ManagedTask for Route<R> {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}
