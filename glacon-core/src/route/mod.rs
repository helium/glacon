//! A route: records read from N inputs, transformed by a middleware and
//! handed to M outputs, each keeping what it wants.
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
//! Up: the route opens its ledger (`<state_dir>/state.db`), then outputs,
//! middlewares and inputs are initialized, layer by layer, before any task
//! starts. Down: the TaskManager stops inputs first,
//! then their background tasks, then processors (handling what is queued),
//! then outputs (writing what is queued, then `terminate`), then their
//! background tasks.

mod builder;
mod fresh_state;
mod settings;

pub use builder::{RouteBuilder, RouteOutputs};
pub use fresh_state::FreshState;
pub use settings::RouteSettings;

use crate::{
    BoxError, Error, ErrorChain, GlaconRecord, InputContext, MiddlewareContext, OutputContext,
    Result,
    input::{self, InitInput, PendingInput, RouteInput},
    ledger::{InputLedger, Ledger},
    lifecycle::{Initialized, init_all, terminate_all},
    output::{self, InitOutput, PendingOutput, RouteOutput},
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

/// A validated route, ready to run as a task. Its inputs produce `In`
/// records, which its middleware turns into the `Out` records its outputs
/// get (the same type without a middleware).
pub struct Route<In, Out = In> {
    name: Arc<str>,
    /// Taken once, when the route starts. Behind a mutex because
    /// `ManagedTask` requires `Sync` and stages only need `Send`.
    parts: Mutex<RouteParts<In, Out>>,
}

struct RouteParts<In, Out> {
    inputs: Vec<(String, Box<dyn InitInput<In>>)>,
    middleware: MiddlewareFactory<In, Out>,
    outputs: Vec<(String, Box<dyn InitOutput<Out>>)>,
    settings: RouteSettings,
    fresh_state: FreshState,
}

impl<In: GlaconRecord, Out: GlaconRecord> Route<In, Out> {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<In: GlaconRecord, Out: GlaconRecord> ManagedTask for Route<In, Out> {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        let Route { name, parts } = *self;
        let parts = parts.into_inner().unwrap_or_else(PoisonError::into_inner);
        task_manager::spawn(run(name, parts, shutdown))
    }
}

async fn run<In: GlaconRecord, Out: GlaconRecord>(
    route: Arc<str>,
    parts: RouteParts<In, Out>,
    shutdown: triggered::Listener,
) -> Result<(), BoxError> {
    let tasks = match up(&route, parts).await {
        Ok(tasks) => tasks,
        Err(error) => {
            tracing::error!(route = %route, error = %ErrorChain(&error), "route failed to start");
            return Err(error.into());
        }
    };
    tracing::info!(route = %route, "route started");

    let result = Box::new(tasks)
        .start_task(shutdown)
        .await
        .map_err(unwrap_task_error);
    match &result {
        Ok(()) => tracing::info!(route = %route, "route stopped"),
        Err(error) => {
            tracing::error!(route = %route, error = %ErrorChain(&**error), "route failed")
        }
    }
    result
}

/// Open the ledger, initialize every stage, layer by layer, then wire them
/// into the route's TaskManager. A failed init terminates the layers already
/// initialized.
async fn up<In: GlaconRecord, Out: GlaconRecord>(
    route: &Arc<str>,
    parts: RouteParts<In, Out>,
) -> Result<TaskManager> {
    let RouteParts {
        inputs,
        middleware,
        outputs,
        settings,
        fresh_state,
    } = parts;

    tracing::info!(
        route = %route,
        state_dir = %settings.state_dir.display(),
        processed_retention = %humantime::format_duration(settings.processed_retention),
        inputs = %inputs.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>().join(","),
        outputs = %outputs.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>().join(","),
        fresh_state = ?fresh_state,
        "starting route"
    );
    let ledger = Arc::new(Ledger::open(&settings.state_dir).await?);
    let fresh = ledger.is_fresh().await?;
    tracing::info!(route = %route, fresh, "ledger opened");

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

    let with_data = outputs
        .iter()
        .find(|(output, _)| output.has_data)
        .map(|(output, _)| output.ctx.output.clone());
    if let Err(error) = guard_fresh_state(route, &ledger, fresh, fresh_state, with_data).await {
        terminate_all(stages(outputs)).await;
        return Err(error);
    }

    let middlewares = inputs
        .iter()
        .map(|(name, _)| RouteMiddleware {
            ctx: MiddlewareContext {
                route: route.clone(),
                input: name.as_str().into(),
            },
            middleware: middleware(),
        })
        .collect();
    // Middlewares have no background task.
    let middlewares: Vec<_> = match init_all(middlewares).await {
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
                    input: name.as_str().into(),
                    ledger: InputLedger::new(
                        route.clone(),
                        name.into(),
                        ledger.clone(),
                        settings.processed_retention,
                    ),
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

/// With an empty ledger, every input starts over: refuse if an output
/// (`with_data`) already holds data, unless told to proceed. Then mark the
/// ledger initialized, so the next start isn't fresh.
async fn guard_fresh_state(
    route: &str,
    ledger: &Ledger,
    fresh: bool,
    fresh_state: FreshState,
    with_data: Option<Arc<str>>,
) -> Result {
    if !fresh {
        return Ok(());
    }
    if let Some(name) = with_data {
        match fresh_state {
            FreshState::Refuse => {
                return Err(Error::FreshStateRefused {
                    route: route.to_string(),
                    output: name.to_string(),
                });
            }
            FreshState::Proceed => tracing::warn!(
                route = %route,
                output = %name,
                "output already has data but the route's state is empty: proceeding, inputs start over"
            ),
        }
    }
    ledger.mark_initialized().await
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
fn wire<In: GlaconRecord, Out: GlaconRecord>(
    route: &Arc<str>,
    inputs: Vec<Initialized<RouteInput<In>>>,
    middlewares: Vec<RouteMiddleware<In, Out>>,
    outputs: Vec<Initialized<RouteOutput<Out>>>,
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
        let ledger = input.ctx.ledger.clone();
        let name = format!("processor {}", middleware.ctx.input);
        tasks = tasks.add_named(
            name.clone(),
            RouteTask::new(route, name, move |shutdown| {
                processor::run(
                    middleware,
                    ledger,
                    received,
                    wait_for_input,
                    senders,
                    shutdown,
                )
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
