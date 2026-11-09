use super::{GlaconOutput, IcebergOutputSettings};
use crate::metrics;
use glacon_core::{
    BatchId, BoxError, GlaconCoreOutput, GlaconRecord, InitializedOutput, OutputContext,
    RecordResult,
};
use helium_iceberg::{BatchedWriter, BatchedWriterConfig};

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
    settings: IcebergOutputSettings,
}

impl<O> IcebergOutput<O> {
    pub fn new(output: O, settings: IcebergOutputSettings) -> Self {
        Self { output, settings }
    }
}

#[async_trait::async_trait]
impl<O, R> GlaconCoreOutput<R> for IcebergOutput<O>
where
    O: GlaconOutput<R>,
    R: GlaconRecord,
{
    type State = IcebergState<O::Row>;

    /// Connect the catalog, create the table if missing and build its
    /// writer, spooling to `<spool_dir>/<route>/<output>`. The writer's task
    /// is returned for the route to run. Reports existing data when the
    /// table existed with a snapshot.
    async fn init(
        &mut self,
        ctx: &OutputContext,
    ) -> Result<InitializedOutput<Self::State>, BoxError> {
        let settings = &self.settings;
        let catalog = settings.catalog.connect().await?;
        let definition = self.output.table()?;
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

        let state = IcebergState {
            ctx: ctx.clone(),
            writer,
        };
        Ok(InitializedOutput::new(state)
            .with_task(task)
            .with_existing_data(has_data))
    }

    /// Turn the records, and the middleware's errors, into rows and queue
    /// them; returns once they are in the spool. Records without rows are
    /// the ones this table drops: a batch with none at all queues nothing.
    async fn handle_data(
        &mut self,
        state: &mut Self::State,
        _: &BatchId,
        records: &[RecordResult<R>],
    ) -> Result<(), BoxError> {
        let mut rows = Vec::new();
        for record in records {
            match record {
                Ok(record) => self.output.to_rows(record, &mut rows),
                Err(error) => self.output.to_error_rows(error, &mut rows),
            }
        }
        if rows.is_empty() {
            return Ok(());
        }
        let count = rows.len() as u64;
        state.writer.queue_all(rows).await?;
        metrics::output_counter(
            metrics::OUTPUT_ROWS,
            state.ctx.route(),
            state.ctx.output(),
            count,
        );
        Ok(())
    }
}

/// The state of an [`IcebergOutput`]: its table's batched writer.
pub struct IcebergState<Row> {
    ctx: OutputContext,
    /// The handle to the batched writer. Dropped with this state, which
    /// lets the writer's task commit and exit.
    writer: BatchedWriter<Row>,
}
