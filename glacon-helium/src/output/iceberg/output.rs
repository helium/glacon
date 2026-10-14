use super::{GlaconOutput, OutputSettings};
use crate::metrics;
use glacon_core::{
    BatchId, BoxError, GlaconCoreOutput, GlaconCoreOutputInit, GlaconRecord, InitializedOutput,
    OutputContext,
};
use helium_iceberg::{BatchedWriter, BatchedWriterConfig};
use std::marker::PhantomData;

/// A [`GlaconCoreOutput`] writing one Iceberg table, whose rows a
/// [`GlaconOutput`] makes from records. Rows go through a
/// `helium_iceberg::BatchedWriter`: spooled to disk, then committed in
/// batches by the writer's task, which the route supervises. When the route
/// stops, the writer commits what is spooled.
///
/// A batch counts as delivered once its rows are in the spool, which
/// survives a process crash: the writer commits it on the next start. Two
/// caveats, from the writer:
/// - the spool is only fsynced at commit, so a machine or power loss can
///   drop rows the route counts as delivered;
/// - a crash after a commit but before its spool file is removed commits
///   that file again on the next start (up to `max_batch_rows` duplicated).
///
/// ```ignore
/// route.output("verified", IcebergOutput::new(VerifiedTable, verified_settings));
/// ```
pub struct IcebergOutput<O> {
    output: O,
    settings: OutputSettings,
}

impl<O> IcebergOutput<O> {
    pub fn new(output: O, settings: OutputSettings) -> Self {
        Self { output, settings }
    }
}

#[async_trait::async_trait]
impl<O, R> GlaconCoreOutputInit<R> for IcebergOutput<O>
where
    O: GlaconOutput<R>,
    R: GlaconRecord,
{
    /// Connect the catalog, create the table if missing and build its
    /// writer, spooling to `<spool_dir>/<route>/<output>`. The writer's task
    /// is returned for the route to run. Reports existing data when the
    /// table existed with a snapshot.
    async fn init(self: Box<Self>, ctx: &OutputContext) -> Result<InitializedOutput<R>, BoxError> {
        let IcebergOutput { output, settings } = *self;
        let catalog = settings.catalog.connect().await?;
        let definition = output.table()?;
        let (namespace, name) = (definition.namespace(), definition.name());
        catalog.create_namespace_if_not_exists(namespace).await?;
        let has_data = catalog.table_exists(namespace, name).await?
            && catalog
                .load_table(namespace, name)
                .await?
                .metadata()
                .current_snapshot()
                .is_some();
        let table = catalog
            .create_table_if_not_exists::<O::Row>(definition)
            .await?;

        let spool_dir = settings.spool_dir.join(ctx.route()).join(ctx.output());
        let config = BatchedWriterConfig::new(spool_dir)
            .with_max_batch_size(settings.max_batch_rows)
            .with_batch_timeout(settings.batch_timeout);
        let (writer, task) = BatchedWriter::new(table, config);

        let running = RunningIcebergOutput {
            output,
            ctx: ctx.clone(),
            writer,
            _record: PhantomData,
        };
        Ok(InitializedOutput::new(running)
            .with_task(task)
            .with_existing_data(has_data))
    }
}

/// An [`IcebergOutput`] once initialized: turns records into rows and queues
/// them to the table's batched writer.
struct RunningIcebergOutput<O: GlaconOutput<R>, R> {
    output: O,
    ctx: OutputContext,
    /// The handle to the batched writer. Dropped with this writer, which
    /// lets the writer's task commit and exit.
    writer: BatchedWriter<O::Row>,
    _record: PhantomData<fn(&R)>,
}

#[async_trait::async_trait]
impl<O, R> GlaconCoreOutput<R> for RunningIcebergOutput<O, R>
where
    O: GlaconOutput<R>,
    R: GlaconRecord,
{
    /// Turn the records into rows and queue them; returns once they are in
    /// the spool.
    async fn handle_data(&mut self, _: &BatchId, records: &[&R]) -> Result<(), BoxError> {
        let mut rows = Vec::new();
        for record in records {
            self.output.to_rows(record, &mut rows);
        }
        let count = rows.len() as u64;
        self.writer.queue_all(rows).await?;
        metrics::output_counter(
            metrics::OUTPUT_ROWS,
            self.ctx.route(),
            self.ctx.output(),
            count,
        );
        Ok(())
    }
}
