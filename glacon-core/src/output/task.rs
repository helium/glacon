use super::{Delivery, GlaconCoreOutput, InitializedOutput, OutputContext};
use crate::{
    BatchId, BoxError, Error, GlaconRecord, GlaconRecordError, RecordResult, Result,
    lifecycle::{Initialized, Lifecycle, Terminate},
    metrics,
};
use std::time::Instant;
use tokio::sync::mpsc;

/// Your output before `init`, with its context.
pub(crate) struct PendingOutput<R, E> {
    pub ctx: OutputContext,
    pub init: Box<dyn InitOutput<R, E>>,
}

/// Your output after `init`, with its state: what its task writes to.
pub(crate) struct RouteOutput<R, E> {
    pub ctx: OutputContext,
    pub output: Box<dyn WriteOutput<R, E>>,
    /// The destination already held data when initialized.
    pub has_data: bool,
}

/// Any [`GlaconCoreOutput`] before `init`, whatever its state. Returns it
/// started, and whether the destination already held data.
#[async_trait::async_trait]
pub(crate) trait InitOutput<R, E>: Send {
    async fn init(
        self: Box<Self>,
        ctx: &OutputContext,
    ) -> Result<(Initialized<Box<dyn WriteOutput<R, E>>>, bool), BoxError>;
}

/// Any [`GlaconCoreOutput`] after `init`, holding its state.
#[async_trait::async_trait]
pub(crate) trait WriteOutput<R, E>: Send {
    async fn handle_data(
        &mut self,
        batch: &BatchId,
        records: &[RecordResult<R, E>],
    ) -> Result<(), BoxError>;
    async fn already_written(&mut self, batch: &BatchId) -> Result<bool, BoxError>;
    async fn terminate(&mut self) -> Result<(), BoxError>;
}

/// An output with the state its `init` built.
struct Started<O: GlaconCoreOutput<R, E>, R: GlaconRecord, E: GlaconRecordError> {
    output: O,
    state: O::State,
}

#[async_trait::async_trait]
impl<R: GlaconRecord, E: GlaconRecordError, O: GlaconCoreOutput<R, E>> InitOutput<R, E> for O {
    async fn init(
        mut self: Box<Self>,
        ctx: &OutputContext,
    ) -> Result<(Initialized<Box<dyn WriteOutput<R, E>>>, bool), BoxError> {
        let InitializedOutput {
            state,
            task,
            has_data,
        } = GlaconCoreOutput::init(&mut *self, ctx).await?;
        let started = Started {
            output: *self,
            state,
        };
        Ok(((Box::new(started), task), has_data))
    }
}

#[async_trait::async_trait]
impl<R: GlaconRecord, E: GlaconRecordError, O: GlaconCoreOutput<R, E>> WriteOutput<R, E>
    for Started<O, R, E>
{
    async fn handle_data(
        &mut self,
        batch: &BatchId,
        records: &[RecordResult<R, E>],
    ) -> Result<(), BoxError> {
        self.output
            .handle_data(&mut self.state, batch, records)
            .await
    }

    async fn already_written(&mut self, batch: &BatchId) -> Result<bool, BoxError> {
        self.output.already_written(&mut self.state, batch).await
    }

    async fn terminate(&mut self) -> Result<(), BoxError> {
        self.output.terminate(&mut self.state).await
    }
}

#[async_trait::async_trait]
impl<R: GlaconRecord, E: GlaconRecordError> Lifecycle for PendingOutput<R, E> {
    type Started = RouteOutput<R, E>;

    async fn init(self) -> Result<Initialized<RouteOutput<R, E>>> {
        let PendingOutput { ctx, init } = self;
        // Logged before init too: an init stuck on its destination (a
        // catalog that never answers) would otherwise log nothing at all.
        tracing::info!(route = %ctx.route, output = %ctx.output, "initializing output");
        let started = Instant::now();
        let ((output, task), has_data) = init
            .init(&ctx)
            .await
            .map_err(|e| Error::output(&ctx.route, &ctx.output, e))?;
        tracing::info!(
            route = %ctx.route,
            output = %ctx.output,
            has_data,
            background_task = task.is_some(),
            took_ms = started.elapsed().as_millis() as u64,
            "output initialized"
        );
        Ok((
            RouteOutput {
                ctx,
                output,
                has_data,
            },
            task,
        ))
    }
}

#[async_trait::async_trait]
impl<R: GlaconRecord, E: GlaconRecordError> Terminate for RouteOutput<R, E> {
    async fn terminate(&mut self) -> Result {
        self.output
            .terminate()
            .await
            .map_err(|e| Error::output(&self.ctx.route, &self.ctx.output, e))
    }
}

/// The output's task: write every delivery until all processors stopped (or,
/// on shutdown, until what is queued is written), then terminate the output.
///
/// Each written batch is recorded in the ledger as delivered to this output.
/// A replayed batch the output says it already has is only recorded.
pub(crate) async fn run<R: GlaconRecord, E: GlaconRecordError>(
    mut route_output: RouteOutput<R, E>,
    mut deliveries: mpsc::Receiver<Delivery<R, E>>,
    shutdown: triggered::Listener,
) -> Result {
    let written = write(&mut route_output, &mut deliveries, &shutdown).await;
    // Processors still sending (only after this output failed) see it gone.
    drop(deliveries);
    let terminated = route_output.terminate().await;
    written.and(terminated)
}

async fn write<R: GlaconRecord, E: GlaconRecordError>(
    route_output: &mut RouteOutput<R, E>,
    deliveries: &mut mpsc::Receiver<Delivery<R, E>>,
    shutdown: &triggered::Listener,
) -> Result {
    let OutputContext { route, output } = &route_output.ctx;
    let mut closed = false;
    loop {
        let delivery = tokio::select! {
            biased;
            delivery = deliveries.recv() => match delivery {
                Some(delivery) => delivery,
                None => return Ok(()),
            },
            // Processors stop first; take what they left, then stop.
            _ = shutdown.clone(), if !closed => {
                deliveries.close();
                closed = true;
                continue;
            }
        };
        let Delivery { ticket, records } = delivery;
        let written = ticket.replayed
            && route_output
                .output
                .already_written(&ticket.id)
                .await
                .map_err(|e| Error::output(route, output, e))?;
        if written {
            metrics::skipped_delivery(route, output, metrics::SKIPPED_ALREADY_WRITTEN);
            tracing::info!(route = %route, output = %output, batch = %ticket.id.key, "already written");
        } else {
            let started = Instant::now();
            route_output
                .output
                .handle_data(&ticket.id, &records)
                .await
                .map_err(|e| Error::output(route, output, e))?;
            let took = started.elapsed();
            let err = records.iter().filter(|r| r.is_err()).count() as u64;
            let ok = records.len() as u64 - err;
            metrics::output_wrote(route, output, ok, err, took);
            tracing::debug!(
                route = %route,
                output = %output,
                batch = %ticket.id.key,
                ok,
                err,
                took_ms = took.as_millis() as u64,
                "wrote batch"
            );
        }
        ticket.delivered(output).await?;
    }
}
