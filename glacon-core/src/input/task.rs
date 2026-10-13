use super::{Batch, GlaconCoreInput, InputContext};
use crate::{Error, GlaconRecord, Result, lifecycle::Lifecycle};
use tokio::sync::{mpsc, oneshot};

/// Your input as the route manages it: with its context, behind
/// [`Lifecycle`].
pub(crate) struct RouteInput<R> {
    pub ctx: InputContext,
    pub input: Box<dyn GlaconCoreInput<R>>,
}

#[async_trait::async_trait]
impl<R: GlaconRecord> Lifecycle for RouteInput<R> {
    fn describe(&self) -> String {
        format!("input {}", self.ctx.input)
    }

    async fn init(&mut self) -> Result {
        self.input
            .init(&self.ctx)
            .await
            .map_err(|e| Error::input(&self.ctx.route, &self.ctx.input, e))
    }

    async fn terminate(&mut self) -> Result {
        self.input
            .terminate()
            .await
            .map_err(|e| Error::input(&self.ctx.route, &self.ctx.input, e))
    }
}

/// The input's task: read batches into the processor's channel until the
/// input is done or the route stops, then terminate the input.
///
/// `stopped` is dropped once the input terminated: its processor waits for
/// it before terminating the middleware.
pub(crate) async fn run<R: GlaconRecord>(
    mut route_input: RouteInput<R>,
    batches: mpsc::Sender<Batch<R>>,
    stopped: oneshot::Sender<()>,
    shutdown: triggered::Listener,
) -> Result {
    let read = read(&mut route_input, &batches, &shutdown).await;
    let terminated = route_input.terminate().await;
    drop((batches, stopped));
    read.and(terminated)
}

async fn read<R: GlaconRecord>(
    route_input: &mut RouteInput<R>,
    batches: &mpsc::Sender<Batch<R>>,
    shutdown: &triggered::Listener,
) -> Result {
    let InputContext { route, input } = &route_input.ctx;
    // The processor stops its input when it fails (or loses an output),
    // which the route reports.
    let processor_stopped = || {
        tracing::warn!(route = %route, input = %input, "processor stopped, stopping input");
        Ok(())
    };
    loop {
        let next = tokio::select! {
            biased;
            _ = shutdown.clone() => return Ok(()),
            _ = batches.closed() => return processor_stopped(),
            next = route_input.input.next_batch() => next.map_err(|e| Error::input(route, input, e))?,
        };
        let Some(batch) = next else {
            tracing::info!(route = %route, input = %input, "input finished");
            return Ok(());
        };
        tokio::select! {
            biased;
            _ = shutdown.clone() => return Ok(()),
            sent = batches.send(batch) => if sent.is_err() {
                return processor_stopped();
            },
        }
    }
}
