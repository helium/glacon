use super::{Batch, GlaconCoreInput, InitializedInput, InputContext, ReadBatch};
use crate::{
    BoxError, Error, GlaconRecord, Result,
    ledger::Read,
    lifecycle::{Initialized, Lifecycle, Terminate},
    metrics,
};
use tokio::sync::{mpsc, oneshot};

/// Your input before `init`, with its context.
pub(crate) struct PendingInput<R> {
    pub ctx: InputContext,
    pub init: Box<dyn InitInput<R>>,
}

/// Your input after `init`, with its state: what its task reads.
pub(crate) struct RouteInput<R> {
    pub ctx: InputContext,
    pub input: Box<dyn ReadInput<R>>,
}

/// Any [`GlaconCoreInput`] before `init`, whatever its state.
#[async_trait::async_trait]
pub(crate) trait InitInput<R>: Send {
    async fn init(
        self: Box<Self>,
        ctx: &InputContext,
    ) -> Result<Initialized<Box<dyn ReadInput<R>>>, BoxError>;
}

/// Any [`GlaconCoreInput`] after `init`, holding its state.
#[async_trait::async_trait]
pub(crate) trait ReadInput<R>: Send {
    async fn next_batch(&mut self) -> Result<Option<Batch<R>>, BoxError>;
}

/// An input with the state its `init` built.
struct Started<I: GlaconCoreInput<R>, R: GlaconRecord> {
    input: I,
    state: I::State,
}

#[async_trait::async_trait]
impl<R: GlaconRecord, I: GlaconCoreInput<R>> InitInput<R> for I {
    async fn init(
        mut self: Box<Self>,
        ctx: &InputContext,
    ) -> Result<Initialized<Box<dyn ReadInput<R>>>, BoxError> {
        let InitializedInput { state, task } = GlaconCoreInput::init(&mut *self, ctx).await?;
        let started = Started {
            input: *self,
            state,
        };
        Ok((Box::new(started), task))
    }
}

#[async_trait::async_trait]
impl<R: GlaconRecord, I: GlaconCoreInput<R>> ReadInput<R> for Started<I, R> {
    async fn next_batch(&mut self) -> Result<Option<Batch<R>>, BoxError> {
        self.input.next_batch(&mut self.state).await
    }
}

#[async_trait::async_trait]
impl<R: GlaconRecord> Lifecycle for PendingInput<R> {
    type Started = RouteInput<R>;

    async fn init(self) -> Result<Initialized<RouteInput<R>>> {
        let PendingInput { ctx, init } = self;
        let (input, task) = init
            .init(&ctx)
            .await
            .map_err(|e| Error::input(&ctx.route, &ctx.input, e))?;
        Ok((RouteInput { ctx, input }, task))
    }
}

/// Inputs have nothing to undo: the input and its state are just dropped.
impl<R: GlaconRecord> Terminate for RouteInput<R> {}

/// The input's task: read batches into the processor's channel until the
/// input is done or the route stops.
///
/// Every batch is recorded in the ledger before it is sent: a batch already
/// done is skipped, one read before but not done is sent as a replay.
///
/// `stopped` is dropped once the input is: its processor waits for it
/// before terminating the middleware.
pub(crate) async fn run<R: GlaconRecord>(
    mut route_input: RouteInput<R>,
    batches: mpsc::Sender<ReadBatch<R>>,
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
    batches: &mpsc::Sender<ReadBatch<R>>,
    shutdown: &triggered::Listener,
) -> Result {
    let InputContext {
        route,
        input,
        ledger,
    } = &route_input.ctx;
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
        let Some(Batch {
            key,
            timestamp,
            records,
        }) = next
        else {
            tracing::info!(route = %route, input = %input, "input finished");
            return Ok(());
        };
        let id = ledger.batch_id(key, timestamp);
        let replayed = match ledger.read(&id).await? {
            Read::First => false,
            Read::Replay => {
                metrics::input_counter(metrics::REPLAYED_BATCHES, route, input, 1);
                tracing::info!(route = %route, input = %input, batch = %id.key, "replaying batch");
                true
            }
            Read::Done => {
                tracing::debug!(route = %route, input = %input, batch = %id.key, "skipping done batch");
                continue;
            }
        };
        let batch = ReadBatch {
            id,
            replayed,
            records,
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
