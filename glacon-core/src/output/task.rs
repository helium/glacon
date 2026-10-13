use super::{Delivery, GlaconCoreOutput, GlaconCoreOutputInit, InitializedOutput, OutputContext};
use crate::{
    Error, GlaconRecord, Result,
    lifecycle::{Initialized, Lifecycle, Terminate},
};
use tokio::sync::mpsc;

/// Your output before `init`, with its context.
pub(crate) struct PendingOutput<R> {
    pub ctx: OutputContext,
    pub init: Box<dyn GlaconCoreOutputInit<R>>,
}

/// Your output after `init`: the running output its task drives.
pub(crate) struct RouteOutput<R> {
    pub ctx: OutputContext,
    pub output: Box<dyn GlaconCoreOutput<R>>,
}

#[async_trait::async_trait]
impl<R: GlaconRecord> Lifecycle for PendingOutput<R> {
    type Started = RouteOutput<R>;

    async fn init(self) -> Result<Initialized<RouteOutput<R>>> {
        let PendingOutput { ctx, init } = self;
        let InitializedOutput { output, task } = init
            .init(&ctx)
            .await
            .map_err(|e| Error::output(&ctx.route, &ctx.output, e))?;
        Ok((RouteOutput { ctx, output }, task))
    }
}

#[async_trait::async_trait]
impl<R: GlaconRecord> Terminate for RouteOutput<R> {
    async fn terminate(&mut self) -> Result {
        self.output
            .terminate()
            .await
            .map_err(|e| Error::output(&self.ctx.route, &self.ctx.output, e))
    }
}

/// The output's task: write every delivery until all processors stopped (or,
/// on shutdown, until what is queued is written), then terminate the output.
pub(crate) async fn run<R: GlaconRecord>(
    mut route_output: RouteOutput<R>,
    mut deliveries: mpsc::Receiver<Delivery<R>>,
    shutdown: triggered::Listener,
) -> Result {
    let written = write(&mut route_output, &mut deliveries, &shutdown).await;
    // Processors still sending (only after this output failed) see it gone.
    drop(deliveries);
    let terminated = route_output.terminate().await;
    written.and(terminated)
}

async fn write<R: GlaconRecord>(
    route_output: &mut RouteOutput<R>,
    deliveries: &mut mpsc::Receiver<Delivery<R>>,
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
        let records: Vec<&R> = delivery
            .picks
            .iter()
            .map(|&i| &delivery.records[i])
            .collect();
        route_output
            .output
            .handle_data(&records)
            .await
            .map_err(|e| Error::output(route, output, e))?;
    }
}
