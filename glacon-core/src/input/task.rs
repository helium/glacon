use super::{Batch, GlaconCoreInput, GlaconCoreInputInit, InitializedInput, InputContext};
use crate::{
    Error, GlaconRecord, Result,
    lifecycle::{Initialized, Lifecycle, Terminate},
};
use tokio::sync::{mpsc, oneshot};

/// Your input before `init`, with its context.
pub(crate) struct PendingInput<R> {
    pub ctx: InputContext,
    pub init: Box<dyn GlaconCoreInputInit<R>>,
}

/// Your input after `init`: the running input its task drives.
pub(crate) struct RouteInput<R> {
    pub ctx: InputContext,
    pub input: Box<dyn GlaconCoreInput<R>>,
}

#[async_trait::async_trait]
impl<R: GlaconRecord> Lifecycle for PendingInput<R> {
    type Started = RouteInput<R>;

    async fn init(self) -> Result<Initialized<RouteInput<R>>> {
        let PendingInput { ctx, init } = self;
        let InitializedInput { input, task } = init
            .init(&ctx)
            .await
            .map_err(|e| Error::input(&ctx.route, &ctx.input, e))?;
        Ok((RouteInput { ctx, input }, task))
    }
}

/// Inputs have nothing to undo: the running input is just dropped.
impl<R: GlaconRecord> Terminate for RouteInput<R> {}

/// The input's task: read batches into the processor's channel until the
/// input is done or the route stops.
///
/// `stopped` is dropped once the input is: its processor waits for it
/// before terminating the middleware.
pub(crate) async fn run<R: GlaconRecord>(
    mut route_input: RouteInput<R>,
    batches: mpsc::Sender<Batch<R>>,
    stopped: oneshot::Sender<()>,
    shutdown: triggered::Listener,
) -> Result {
    let read = read(&mut route_input, &batches, &shutdown).await;
    drop(route_input);
    drop((batches, stopped));
    read
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
