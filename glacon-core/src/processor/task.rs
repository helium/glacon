use super::middleware::{GlaconCoreMiddleware, MiddlewareContext};
use crate::{
    Error, GlaconRecord, Result,
    input::ReadBatch,
    ledger::InputLedger,
    lifecycle::{Initialized, Lifecycle, Terminate},
    metrics,
    output::{BatchTicket, Delivery},
};
use chrono::Utc;
use futures::future;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

/// How often a processor forgets its input's expired batches and reports
/// those in flight.
const HOUSEKEEPING_EVERY: Duration = Duration::from_secs(60 * 60);

/// An input's middleware with its context, the same before and after
/// `init`.
pub(crate) struct RouteMiddleware<In, Out> {
    pub ctx: MiddlewareContext,
    pub middleware: Box<dyn GlaconCoreMiddleware<In, Out>>,
}

#[async_trait::async_trait]
impl<In: GlaconRecord, Out: GlaconRecord> Lifecycle for RouteMiddleware<In, Out> {
    type Started = Self;

    async fn init(mut self) -> Result<Initialized<Self>> {
        self.middleware
            .init(&self.ctx)
            .await
            .map_err(|e| Error::middleware(&self.ctx.route, &self.ctx.input, e))?;
        // Middlewares have no background task.
        Ok((self, None))
    }
}

#[async_trait::async_trait]
impl<In: GlaconRecord, Out: GlaconRecord> Terminate for RouteMiddleware<In, Out> {
    async fn terminate(&mut self) -> Result {
        self.middleware
            .terminate()
            .await
            .map_err(|e| Error::middleware(&self.ctx.route, &self.ctx.input, e))
    }
}

/// Where a processor sends an output's records.
pub(crate) struct OutputSender<R> {
    pub name: Arc<str>,
    pub deliveries: mpsc::Sender<Delivery<R>>,
}

/// The processor's task: run every batch of its input through the
/// middleware and hand the records to the outputs, until the input stopped
/// (or, on shutdown, until what is queued is handled), then terminate the
/// middleware.
///
/// Every output gets every batch, and keeps what it wants, except on a
/// replay the outputs that already have it (the ledger knows which). A batch
/// the middleware empties, or no output still needs, is done right away;
/// otherwise the last output to take it marks it done.
///
/// If the middleware fails or an output is gone, the input is stopped first:
/// the middleware terminates only once its input did.
pub(crate) async fn run<In: GlaconRecord, Out: GlaconRecord>(
    mut route_middleware: RouteMiddleware<In, Out>,
    ledger: InputLedger,
    mut batches: mpsc::Receiver<ReadBatch<In>>,
    input_stopped: oneshot::Receiver<()>,
    outputs: Vec<OutputSender<Out>>,
    shutdown: triggered::Listener,
) -> Result {
    let processed = process_all(
        &mut route_middleware,
        &ledger,
        &mut batches,
        &outputs,
        &shutdown,
    )
    .await;
    // Stops the input if still running (it watches for this; what it left
    // is lost), then waits until it terminated.
    drop(batches);
    let _ = input_stopped.await;
    let terminated = route_middleware.terminate().await;
    // Outputs stop once every processor dropped its senders: they terminate
    // after.
    drop(outputs);
    processed.and(terminated)
}

async fn process_all<In: GlaconRecord, Out: GlaconRecord>(
    route_middleware: &mut RouteMiddleware<In, Out>,
    ledger: &InputLedger,
    batches: &mut mpsc::Receiver<ReadBatch<In>>,
    outputs: &[OutputSender<Out>],
    shutdown: &triggered::Listener,
) -> Result {
    let mut closed = false;
    let mut housekeeping = tokio::time::interval(HOUSEKEEPING_EVERY);
    housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let batch = tokio::select! {
            biased;
            batch = batches.recv() => match batch {
                Some(batch) => batch,
                None => return Ok(()),
            },
            _ = housekeeping.tick() => {
                housekeep(&route_middleware.ctx, ledger).await;
                continue;
            }
            // The input stops first; take what it left, then stop.
            _ = shutdown.clone(), if !closed => {
                batches.close();
                closed = true;
                continue;
            }
        };
        if !process(route_middleware, ledger, batch, outputs).await? {
            return Ok(());
        }
    }
}

/// Forget the input's expired batches and report those in flight. A failure
/// is only logged: the next batch would hit the same ledger and fail the
/// route if it is really broken.
async fn housekeep(ctx: &MiddlewareContext, ledger: &InputLedger) {
    let MiddlewareContext { route, input } = ctx;
    if let Err(error) = ledger.prune_expired().await {
        tracing::warn!(route = %route, input = %input, %error, "failed to prune the ledger");
    }
    match ledger.in_flight().await {
        Ok(n) => metrics::in_flight(route, input, n),
        Err(error) => {
            tracing::warn!(route = %route, input = %input, %error, "failed to count batches in flight")
        }
    }
}

/// Returns `false` once an output stopped: it only does after failing, which
/// the route reports.
async fn process<In: GlaconRecord, Out: GlaconRecord>(
    route_middleware: &mut RouteMiddleware<In, Out>,
    ledger: &InputLedger,
    batch: ReadBatch<In>,
    outputs: &[OutputSender<Out>],
) -> Result<bool> {
    let started = Instant::now();
    let MiddlewareContext { route, input } = &route_middleware.ctx;
    let ReadBatch {
        id,
        replayed,
        records,
    } = batch;

    let read = records.len();
    let records = route_middleware
        .middleware
        .handle_data(&id, records)
        .await
        .map_err(|e| Error::middleware(route, input, e))?;
    let dropped = read.saturating_sub(records.len()) as u64;
    metrics::input_counter(metrics::MIDDLEWARE_DROPPED, route, input, dropped);

    // Every output gets the batch, except, on a replay, those that already
    // took it. A batch left empty goes to none.
    let mut targets: Vec<&OutputSender<Out>> = Vec::new();
    if !records.is_empty() {
        let delivered = if replayed {
            ledger.deliveries(&id).await?
        } else {
            Default::default()
        };
        for output in outputs {
            if delivered.contains(&*output.name) {
                metrics::output_counter(metrics::SKIPPED_DELIVERIES, route, &output.name, 1);
            } else {
                targets.push(output);
            }
        }
    }

    if targets.is_empty() {
        ledger.complete(&id).await?;
    } else {
        let ticket = Arc::new(BatchTicket::new(
            id.clone(),
            replayed,
            ledger.clone(),
            targets.len(),
        ));
        let records = Arc::new(records);
        let sends = targets.iter().map(|output| {
            let delivery = Delivery {
                ticket: ticket.clone(),
                records: records.clone(),
            };
            async move { (output, output.deliveries.send(delivery).await.is_ok()) }
        });
        for (output, sent) in future::join_all(sends).await {
            if !sent {
                tracing::warn!(route = %route, input = %input, output = %output.name, "output stopped, stopping processor");
                return Ok(false);
            }
            metrics::output_counter(
                metrics::OUTPUT_RECORDS,
                route,
                &output.name,
                records.len() as u64,
            );
        }
    }

    let lag = (Utc::now() - id.timestamp).to_std().unwrap_or_default();
    metrics::batch_processed(route, input, lag, started.elapsed());
    tracing::info!(
        route = %route,
        input = %input,
        batch = %id.key,
        records = read,
        dropped,
        outputs = targets.len(),
        replayed,
        "processed batch"
    );
    Ok(true)
}
