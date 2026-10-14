use super::middleware::{Destination, GlaconCoreMiddleware, MiddlewareContext, OutputId};
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
pub(crate) struct RouteMiddleware<R> {
    pub ctx: MiddlewareContext,
    pub middleware: Box<dyn GlaconCoreMiddleware<R>>,
}

#[async_trait::async_trait]
impl<R: GlaconRecord> Lifecycle for RouteMiddleware<R> {
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
impl<R: GlaconRecord> Terminate for RouteMiddleware<R> {
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
/// A batch is handed only to the outputs that don't have it yet (on a
/// replay, the ledger knows which do). A batch no output gets is done right
/// away; otherwise the last output to take it marks it done.
///
/// If the middleware fails or an output is gone, the input is stopped first:
/// the middleware terminates only once its input did.
pub(crate) async fn run<R: GlaconRecord>(
    mut route_middleware: RouteMiddleware<R>,
    ledger: InputLedger,
    mut batches: mpsc::Receiver<ReadBatch<R>>,
    input_stopped: oneshot::Receiver<()>,
    outputs: Vec<OutputSender<R>>,
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

async fn process_all<R: GlaconRecord>(
    route_middleware: &mut RouteMiddleware<R>,
    ledger: &InputLedger,
    batches: &mut mpsc::Receiver<ReadBatch<R>>,
    outputs: &[OutputSender<R>],
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
async fn process<R: GlaconRecord>(
    route_middleware: &mut RouteMiddleware<R>,
    ledger: &InputLedger,
    batch: ReadBatch<R>,
    outputs: &[OutputSender<R>],
) -> Result<bool> {
    let started = Instant::now();
    let MiddlewareContext { route, input } = &route_middleware.ctx;
    let ReadBatch {
        id,
        replayed,
        records,
    } = batch;

    let destinations = route_middleware
        .middleware
        .handle_data(&id, &records)
        .await
        .map_err(|e| Error::middleware(route, input, e))?;
    if destinations.len() != records.len() {
        return Err(Error::middleware(
            route,
            input,
            format!(
                "returned {} destinations for {} records",
                destinations.len(),
                records.len()
            ),
        ));
    }
    let (mut picks, dropped) = pick(&destinations, outputs.len());
    metrics::input_counter(metrics::MIDDLEWARE_DROPPED, route, input, dropped);

    // A replayed batch skips the outputs that already took it.
    if replayed {
        let delivered = ledger.deliveries(&id).await?;
        for (output, picks) in outputs.iter().zip(&mut picks) {
            if !picks.is_empty() && delivered.contains(&*output.name) {
                metrics::output_counter(metrics::SKIPPED_DELIVERIES, route, &output.name, 1);
                picks.clear();
            }
        }
    }

    let targets = picks.iter().filter(|picks| !picks.is_empty()).count();
    let records = Arc::new(records);
    if targets == 0 {
        ledger.complete(&id).await?;
    }
    let ticket = Arc::new(BatchTicket::new(
        id.clone(),
        replayed,
        ledger.clone(),
        targets,
    ));
    let sends = outputs
        .iter()
        .zip(picks)
        .filter(|(_, picks)| !picks.is_empty())
        .map(|(output, picks)| {
            let delivery = Delivery {
                ticket: ticket.clone(),
                records: records.clone(),
                picks,
            };
            async move {
                let routed = delivery.picks.len() as u64;
                let sent = output.deliveries.send(delivery).await;
                (output, routed, sent.is_ok())
            }
        });
    for (output, routed, sent) in future::join_all(sends).await {
        if !sent {
            tracing::warn!(route = %route, input = %input, output = %output.name, "output stopped, stopping processor");
            return Ok(false);
        }
        metrics::output_counter(metrics::OUTPUT_RECORDS, route, &output.name, routed);
    }

    let lag = (Utc::now() - id.timestamp).to_std().unwrap_or_default();
    metrics::batch_processed(route, input, lag, started.elapsed());
    tracing::info!(
        route = %route,
        input = %input,
        batch = %id.key,
        records = records.len(),
        dropped,
        outputs = targets,
        replayed,
        "processed batch"
    );
    Ok(true)
}

/// Turn one destination per record into, per output, the positions of its
/// records. Also returns how many records go nowhere.
fn pick(destinations: &[Destination], outputs: usize) -> (Vec<Vec<usize>>, u64) {
    let mut picks = vec![Vec::new(); outputs];
    let mut dropped = 0u64;
    for (i, destination) in destinations.iter().enumerate() {
        match destination {
            Destination::All => picks.iter_mut().for_each(|p| p.push(i)),
            Destination::Only(set) if !set.is_empty() => {
                for (output, p) in picks.iter_mut().enumerate() {
                    if set.contains(OutputId(output as u8)) {
                        p.push(i);
                    }
                }
            }
            Destination::Only(_) | Destination::Drop => dropped += 1,
        }
    }
    (picks, dropped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OutputSet;

    #[test]
    fn picks_records_per_output() {
        let destinations = [
            Destination::All,
            Destination::Only(OutputId(1).into()),
            Destination::Drop,
            Destination::Only(OutputSet::new()),
            Destination::Only(OutputId(0).into()),
        ];
        let (picks, dropped) = pick(&destinations, 2);
        assert_eq!(picks, [vec![0, 4], vec![0, 1]]);
        assert_eq!(dropped, 2);
    }
}
