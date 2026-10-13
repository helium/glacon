use super::middleware::{Destination, GlaconCoreMiddleware, MiddlewareContext, OutputId};
use crate::{Batch, Error, GlaconRecord, Result, lifecycle::Lifecycle, metrics, output::Delivery};
use chrono::Utc;
use futures::future;
use std::{sync::Arc, time::Instant};
use tokio::sync::{mpsc, oneshot};

/// An input's middleware as the route manages it: with its context, behind
/// [`Lifecycle`].
pub(crate) struct RouteMiddleware<R> {
    pub ctx: MiddlewareContext,
    pub middleware: Box<dyn GlaconCoreMiddleware<R>>,
}

#[async_trait::async_trait]
impl<R: GlaconRecord> Lifecycle for RouteMiddleware<R> {
    fn describe(&self) -> String {
        format!("middleware of input {}", self.ctx.input)
    }

    async fn init(&mut self) -> Result {
        self.middleware
            .init(&self.ctx)
            .await
            .map_err(|e| Error::middleware(&self.ctx.route, &self.ctx.input, e))
    }

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
/// If the middleware fails or an output is gone, the input is stopped first:
/// the middleware terminates only once its input did.
pub(crate) async fn run<R: GlaconRecord>(
    mut route_middleware: RouteMiddleware<R>,
    mut batches: mpsc::Receiver<Batch<R>>,
    input_stopped: oneshot::Receiver<()>,
    outputs: Vec<OutputSender<R>>,
    shutdown: triggered::Listener,
) -> Result {
    let processed = process_all(&mut route_middleware, &mut batches, &outputs, &shutdown).await;
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
    batches: &mut mpsc::Receiver<Batch<R>>,
    outputs: &[OutputSender<R>],
    shutdown: &triggered::Listener,
) -> Result {
    let mut closed = false;
    loop {
        let batch = tokio::select! {
            biased;
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
        if !process(route_middleware, batch, outputs).await? {
            return Ok(());
        }
    }
}

/// Returns `false` once an output stopped: it only does after failing, which
/// the route reports.
async fn process<R: GlaconRecord>(
    route_middleware: &mut RouteMiddleware<R>,
    batch: Batch<R>,
    outputs: &[OutputSender<R>],
) -> Result<bool> {
    let started = Instant::now();
    let MiddlewareContext { route, input } = &route_middleware.ctx;
    let Batch {
        key,
        timestamp,
        records,
    } = batch;

    let destinations = route_middleware
        .middleware
        .handle_data(&records)
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
    let (picks, dropped) = pick(&destinations, outputs.len());
    metrics::input_counter(metrics::MIDDLEWARE_DROPPED, route, input, dropped);

    let records = Arc::new(records);
    let sends = outputs
        .iter()
        .zip(picks)
        .filter(|(_, picks)| !picks.is_empty())
        .map(|(output, picks)| {
            let records = records.clone();
            async move {
                let routed = picks.len() as u64;
                let sent = output.deliveries.send(Delivery { records, picks }).await;
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

    let lag = (Utc::now() - timestamp).to_std().unwrap_or_default();
    metrics::batch_processed(route, input, lag, started.elapsed());
    tracing::info!(
        route = %route,
        input = %input,
        batch = %key,
        records = records.len(),
        dropped,
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
