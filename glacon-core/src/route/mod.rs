//! A route: one record type read from N inputs, run through a middleware and
//! written to M outputs.
//!
//! A [`Route`] is an oracles `ManagedTask`: run it in your app's
//! `TaskManager`. It runs its own TaskManager, one task per stage, joined by
//! channels, plus the background task each input or output may return from
//! `init`:
//!
//! ```text
//! input task ──► processor task (middleware) ──┬──► output task
//! input task ──► processor task (middleware) ──┴──► output task
//! ```
//!
//! Up: outputs, then middlewares, then inputs are initialized, layer by
//! layer, before any task starts. Down: the TaskManager stops inputs first,
//! then their background tasks, then processors (handling what is queued),
//! then outputs (writing what is queued, then `terminate`), then their
//! background tasks.

mod builder;

pub use builder::RouteBuilder;

use crate::{
    BoxError, GlaconCoreInputInit, GlaconCoreOutputInit, GlaconRecord, InputContext,
    MiddlewareContext, OutputContext, OutputIndex, Result,
    input::{self, PendingInput, RouteInput},
    lifecycle::{Initialized, init_all, terminate_all},
    output::{self, PendingOutput, RouteOutput},
    processor::{self, OutputSender, RouteMiddleware},
    task::{BackgroundTask, Owner, RouteTask, unwrap_task_error},
};
use builder::MiddlewareFactory;
use std::sync::{Arc, Mutex, PoisonError};
use task_manager::{ManagedTask, TaskFuture, TaskManager};
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
    inputs: Vec<(String, Box<dyn GlaconCoreInputInit<R>>)>,
    middleware: MiddlewareFactory<R>,
    index: OutputIndex,
    outputs: Vec<(String, Box<dyn GlaconCoreOutputInit<R>>)>,
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
            .map(|(name, init)| PendingOutput {
                ctx: OutputContext {
                    route: route.clone(),
                    output: name.into(),
                },
                init,
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
    // Middlewares have no background task.
    let middlewares: Vec<_> = match middlewares {
        Ok(middlewares) => middlewares.into_iter().map(|(m, _)| m).collect(),
        Err(error) => {
            terminate_all(stages(outputs)).await;
            return Err(error);
        }
    };

    let inputs = init_all(
        inputs
            .into_iter()
            .map(|(name, init)| PendingInput {
                ctx: InputContext {
                    route: route.clone(),
                    input: name.into(),
                },
                init,
            })
            .collect(),
    )
    .await;
    let inputs = match inputs {
        Ok(inputs) => inputs,
        Err(error) => {
            terminate_all(middlewares).await;
            terminate_all(stages(outputs)).await;
            return Err(error);
        }
    };

    Ok(wire(route, inputs, middlewares, outputs))
}

/// The stages, without the background tasks they returned: those are
/// dropped, never started.
fn stages<S>(started: Vec<Initialized<S>>) -> Vec<S> {
    started.into_iter().map(|(stage, _task)| stage).collect()
}

/// One task per stage, joined by channels, plus the background tasks inputs
/// and outputs returned. Tasks start in this order and stop in reverse:
///
/// ```text
/// output backgrounds, outputs, processors, input backgrounds, inputs
/// ```
///
/// so a background task starts before its stage and stops after it.
fn wire<R: GlaconRecord>(
    route: &Arc<str>,
    inputs: Vec<Initialized<RouteInput<R>>>,
    middlewares: Vec<RouteMiddleware<R>>,
    outputs: Vec<Initialized<RouteOutput<R>>>,
) -> TaskManager {
    let mut tasks = TaskManager::builder();

    let (outputs, output_backgrounds): (Vec<_>, Vec<_>) = outputs.into_iter().unzip();
    for (route_output, task) in outputs.iter().zip(output_backgrounds) {
        if let Some(task) = task {
            let owner = Owner::Output(route_output.ctx.output.clone());
            tasks = tasks.add_named(owner.task_name(), BackgroundTask::new(route, owner, task));
        }
    }

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

    let (inputs, input_backgrounds): (Vec<_>, Vec<_>) = inputs.into_iter().unzip();
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

    for ((input, _, _), task) in input_tasks.iter().zip(input_backgrounds) {
        if let Some(task) = task {
            let owner = Owner::Input(input.ctx.input.clone());
            tasks = tasks.add_named(owner.task_name(), BackgroundTask::new(route, owner, task));
        }
    }

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
