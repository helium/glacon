use super::middleware::{GlaconCoreMiddleware, MiddlewareContext};
use crate::{
    Error, ErrorChain, GlaconRecord, RecordResult, Result,
    input::ReadBatch,
    ledger::InputLedger,
    lifecycle::{Initialized, Lifecycle, Terminate},
    metrics,
    output::{BatchTicket, Delivery},
};
use chrono::{DateTime, Utc};
use futures::future;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

/// How often a processor forgets its input's expired batches.
const PRUNE_EVERY: Duration = Duration::from_secs(60 * 60);

/// How often a processor reports its input's batches in flight.
const IN_FLIGHT_EVERY: Duration = Duration::from_secs(60);

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
        let started = Instant::now();
        self.middleware
            .init(&self.ctx)
            .await
            .map_err(|e| Error::middleware(&self.ctx.route, &self.ctx.input, e))?;
        tracing::info!(
            route = %self.ctx.route,
            input = %self.ctx.input,
            took_ms = started.elapsed().as_millis() as u64,
            "middleware initialized"
        );
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
/// Every output gets every batch, failures included, and keeps what it
/// wants, except on a replay the outputs that already have it (the ledger
/// knows which). An empty batch, or one no output still needs, is done right
/// away;
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
    let mut newest = None;
    let mut prune = tokio::time::interval(PRUNE_EVERY);
    prune.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut in_flight = tokio::time::interval(IN_FLIGHT_EVERY);
    in_flight.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        // Timers first: while the input keeps a batch waiting (catching up
        // on a backlog), a batch is always ready, and housekeeping placed
        // after it would never run. A timer is ready at most once per
        // period, so batches still get nearly all the turns.
        let batch = tokio::select! {
            biased;
            _ = prune.tick() => {
                prune_expired(&route_middleware.ctx, ledger).await;
                continue;
            }
            _ = in_flight.tick() => {
                report_in_flight(&route_middleware.ctx, ledger).await;
                continue;
            }
            batch = batches.recv() => match batch {
                Some(batch) => batch,
                None => return Ok(()),
            },
            // The input stops first; take what it left, then stop.
            _ = shutdown.clone(), if !closed => {
                batches.close();
                closed = true;
                continue;
            }
        };
        if !process(route_middleware, ledger, batch, outputs, &mut newest).await? {
            return Ok(());
        }
    }
}

/// Forget the input's expired batches. A failure is only logged: the next
/// batch would hit the same ledger and fail the route if it is really
/// broken.
async fn prune_expired(ctx: &MiddlewareContext, ledger: &InputLedger) {
    let MiddlewareContext { route, input } = ctx;
    match ledger.prune_expired().await {
        Ok(pruned) => {
            tracing::debug!(route = %route, input = %input, pruned, "pruned the ledger")
        }
        Err(error) => tracing::warn!(
            route = %route,
            input = %input,
            error = %ErrorChain(&error),
            "failed to prune the ledger"
        ),
    }
}

/// Report the input's batches in flight; a failure is only logged.
async fn report_in_flight(ctx: &MiddlewareContext, ledger: &InputLedger) {
    let MiddlewareContext { route, input } = ctx;
    match ledger.in_flight().await {
        Ok(n) => metrics::in_flight(route, input, n),
        Err(error) => tracing::warn!(
            route = %route,
            input = %input,
            error = %ErrorChain(&error),
            "failed to count batches in flight"
        ),
    }
}

/// The middleware's errors in one batch: how many per reason, and the first
/// one, as an example for the log.
#[derive(Default)]
struct BatchErrors {
    by_reason: BTreeMap<&'static str, u64>,
    first: Option<String>,
}

impl BatchErrors {
    fn of<R>(results: &[RecordResult<R>]) -> Self {
        let mut errors = Self::default();
        for error in results.iter().filter_map(|r| r.as_ref().err()) {
            *errors.by_reason.entry(error.reason).or_default() += 1;
            if errors.first.is_none() {
                errors.first = Some(error.message.clone());
            }
        }
        errors
    }

    fn total(&self) -> u64 {
        self.by_reason.values().sum()
    }

    /// `reason=count` pairs, for the log: `duplicate=3 invalid=1`.
    fn summary(&self) -> String {
        self.by_reason
            .iter()
            .map(|(reason, n)| format!("{reason}={n}"))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Returns `false` once an output stopped: it only does after failing, which
/// the route reports. `newest` is the newest batch timestamp processed so
/// far.
async fn process<In: GlaconRecord, Out: GlaconRecord>(
    route_middleware: &mut RouteMiddleware<In, Out>,
    ledger: &InputLedger,
    batch: ReadBatch<In>,
    outputs: &[OutputSender<Out>],
    newest: &mut Option<DateTime<Utc>>,
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
    let middleware_took = started.elapsed();
    if records.len() != read {
        return Err(Error::middleware(
            route,
            input,
            format!(
                "returned {} results for the {read} records of batch {}",
                records.len(),
                id.key
            ),
        ));
    }
    let errors = BatchErrors::of(&records);
    for (reason, n) in &errors.by_reason {
        metrics::middleware_errors(route, input, reason, *n);
    }
    for error in records.iter().filter_map(|r| r.as_ref().err()) {
        tracing::debug!(
            route = %route,
            input = %input,
            batch = %id.key,
            reason = error.reason,
            message = %error.message,
            "record error"
        );
    }

    // Every output gets the batch, except, on a replay, those that already
    // took it. An empty batch goes to none.
    let mut targets: Vec<&OutputSender<Out>> = Vec::new();
    if !records.is_empty() {
        let delivered = if replayed {
            ledger.deliveries(&id).await?
        } else {
            Default::default()
        };
        for output in outputs {
            if delivered.contains(&*output.name) {
                metrics::skipped_delivery(route, &output.name, metrics::SKIPPED_BY_LEDGER);
            } else {
                targets.push(output);
            }
        }
    }

    let handoff = Instant::now();
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
        }
    }
    let handoff_took = handoff.elapsed();

    let newest = *newest.insert(newest.map_or(id.timestamp, |n| n.max(id.timestamp)));
    metrics::batch_processed(
        route,
        input,
        metrics::BatchTimes {
            timestamp: id.timestamp,
            newest,
            middleware: middleware_took,
            handoff: handoff_took,
            total: started.elapsed(),
        },
    );
    tracing::info!(
        route = %route,
        input = %input,
        batch = %id.key,
        records = read,
        errors = errors.total(),
        error_reasons = %errors.summary(),
        first_error = errors.first.as_deref(),
        outputs = targets.len(),
        replayed,
        middleware_ms = middleware_took.as_millis() as u64,
        handoff_ms = handoff_took.as_millis() as u64,
        "processed batch"
    );
    Ok(true)
}
