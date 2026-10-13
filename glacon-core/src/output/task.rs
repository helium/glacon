use super::{Delivery, GlaconCoreOutput, OutputContext};
use crate::{Error, GlaconRecord, Result, lifecycle::Lifecycle};
use tokio::sync::mpsc;

/// Your output as the route manages it: with its context, behind
/// [`Lifecycle`].
pub(crate) struct RouteOutput<R> {
    pub ctx: OutputContext,
    pub output: Box<dyn GlaconCoreOutput<R>>,
}

#[async_trait::async_trait]
impl<R: GlaconRecord> Lifecycle for RouteOutput<R> {
    fn describe(&self) -> String {
        format!("output {}", self.ctx.output)
    }

    async fn init(&mut self) -> Result {
        self.output
            .init(&self.ctx)
            .await
            .map_err(|e| Error::output(&self.ctx.route, &self.ctx.output, e))
    }

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
