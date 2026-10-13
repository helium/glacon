//! A route: one record type read from N inputs, run through a middleware and
//! written to M outputs.
//!
//! A [`Route`] is an oracles `ManagedTask`: run it in your app's
//! `TaskManager`. It runs its own TaskManager, one task per stage, joined by
//! channels:
//!
//! ```text
//! input task ──► processor task (middleware) ──┬──► output task
//! input task ──► processor task (middleware) ──┴──► output task
//! ```
//!
//! Up: outputs, then middlewares, then inputs are initialized, layer by
//! layer, before any task starts. Down: the TaskManager stops inputs first,
//! then processors (handling what is queued), then outputs (writing what is
//! queued); each runs its stage's `terminate` as it stops.

mod builder;

pub use builder::RouteBuilder;

use crate::{
    BoxError, GlaconCoreInput, GlaconCoreOutput, GlaconRecord, InputContext, MiddlewareContext,
    OutputContext, OutputIndex, Result,
    input::{self, RouteInput},
    lifecycle::{init_all, terminate_all},
    output::{self, RouteOutput},
    processor::{self, OutputSender, RouteMiddleware},
    task::RouteTask,
};
use builder::MiddlewareFactory;
use std::sync::{Arc, Mutex, PoisonError};
use task_manager::{ManagedTask, TaskError, TaskFuture, TaskManager};
use tokio::sync::{mpsc, oneshot};

/// Batches waiting in each channel, besides the one being handled: the
/// stage upstream waits beyond that.
const CHANNEL_CAPACITY: usize = 1;

/// A validated route, ready to run as a task.
pub struct Route<R> {
    name: Arc<str>,
    /// Taken once, when the route starts. Behind a mutex because
    /// `ManagedTask` requires `Sync` and stages only need `Send`.
    parts: Mutex<RouteParts<R>>,
}

struct RouteParts<R> {
    inputs: Vec<(String, Box<dyn GlaconCoreInput<R>>)>,
    middleware: MiddlewareFactory<R>,
    index: OutputIndex,
    outputs: Vec<(String, Box<dyn GlaconCoreOutput<R>>)>,
}

impl<R: GlaconRecord> Route<R> {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<R: GlaconRecord> ManagedTask for Route<R> {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        let Route { name, parts } = *self;
        let parts = parts.into_inner().unwrap_or_else(PoisonError::into_inner);
        task_manager::spawn(run(name, parts, shutdown))
    }
}

async fn run<R: GlaconRecord>(
    route: Arc<str>,
    parts: RouteParts<R>,
    shutdown: triggered::Listener,
) -> Result<(), BoxError> {
    let tasks = up(&route, parts).await?;
    tracing::info!(route = %route, "route started");

    let result = Box::new(tasks).start_task(shutdown).await;
    match &result {
        Ok(()) => tracing::info!(route = %route, "route stopped"),
        Err(error) => tracing::error!(route = %route, %error, "route failed"),
    }
    result.map_err(unwrap_task_error)
}

/// The error the failing input, middleware or output returned. A
/// TaskManager run as a task wraps its tasks' errors in another `TaskError`,
/// so unwrap every layer.
fn unwrap_task_error(error: TaskError) -> BoxError {
    match error {
        TaskError::Task(inner) => match inner.downcast::<TaskError>() {
            Ok(nested) => unwrap_task_error(*nested),
            Err(inner) => inner,
        },
        other => other.into(),
    }
}

/// Initialize every stage, layer by layer, then wire them into the route's
/// TaskManager. A failed init terminates the layers already initialized.
async fn up<R: GlaconRecord>(route: &Arc<str>, parts: RouteParts<R>) -> Result<TaskManager> {
    let RouteParts {
        inputs,
        middleware,
        index,
        outputs,
    } = parts;

    let outputs = init_all(
        outputs
            .into_iter()
            .map(|(name, output)| RouteOutput {
                ctx: OutputContext {
                    route: route.clone(),
                    output: name.into(),
                },
                output,
            })
            .collect(),
    )
    .await?;

    let middlewares = inputs
        .iter()
        .map(|(name, _)| {
            Ok(RouteMiddleware {
                ctx: MiddlewareContext {
                    route: route.clone(),
                    input: name.as_str().into(),
                },
                middleware: middleware(&index)?,
            })
        })
        .collect::<Result<Vec<_>>>();
    let middlewares = match middlewares {
        Ok(middlewares) => init_all(middlewares).await,
        Err(error) => Err(error),
    };
    let middlewares = match middlewares {
        Ok(middlewares) => middlewares,
        Err(error) => {
            terminate_all(outputs).await;
            return Err(error);
        }
    };

    let inputs = init_all(
        inputs
            .into_iter()
            .map(|(name, input)| RouteInput {
                ctx: InputContext {
                    route: route.clone(),
                    input: name.into(),
                },
                input,
            })
            .collect(),
    )
    .await;
    let inputs = match inputs {
        Ok(inputs) => inputs,
        Err(error) => {
            terminate_all(middlewares).await;
            terminate_all(outputs).await;
            return Err(error);
        }
    };

    Ok(wire(route, inputs, middlewares, outputs))
}

/// One task per stage, joined by channels. Tasks start in this order and
/// stop in reverse: inputs, processors, outputs.
fn wire<R: GlaconRecord>(
    route: &Arc<str>,
    inputs: Vec<RouteInput<R>>,
    middlewares: Vec<RouteMiddleware<R>>,
    outputs: Vec<RouteOutput<R>>,
) -> TaskManager {
    let mut tasks = TaskManager::builder();

    let mut output_senders = Vec::with_capacity(outputs.len());
    for route_output in outputs {
        let (deliveries, received) = mpsc::channel(CHANNEL_CAPACITY);
        output_senders.push((route_output.ctx.output.clone(), deliveries));
        let name = format!("output {}", route_output.ctx.output);
        tasks = tasks.add_named(
            name.clone(),
            RouteTask::new(route, name, move |shutdown| {
                output::run(route_output, received, shutdown)
            }),
        );
    }

    let mut input_tasks = Vec::with_capacity(inputs.len());
    for (input, middleware) in inputs.into_iter().zip(middlewares) {
        let (batches, received) = mpsc::channel(CHANNEL_CAPACITY);
        let (input_stopped, wait_for_input) = oneshot::channel();
        let senders = output_senders
            .iter()
            .map(|(name, deliveries)| OutputSender {
                name: name.clone(),
                deliveries: deliveries.clone(),
            })
            .collect();
        let name = format!("processor {}", middleware.ctx.input);
        tasks = tasks.add_named(
            name.clone(),
            RouteTask::new(route, name, move |shutdown| {
                processor::run(middleware, received, wait_for_input, senders, shutdown)
            }),
        );
        input_tasks.push((input, batches, input_stopped));
    }
    // Only processors hold output senders now: outputs stop after the last
    // processor does.
    drop(output_senders);

    for (input, batches, stopped) in input_tasks {
        let name = format!("input {}", input.ctx.input);
        tasks = tasks.add_named(
            name.clone(),
            RouteTask::new(route, name, move |shutdown| {
                input::run(input, batches, stopped, shutdown)
            }),
        );
    }
    tasks.build()
}
