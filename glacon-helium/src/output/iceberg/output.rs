use super::{GlaconOutput, OutputSettings};
use crate::metrics;
use glacon_core::{BoxError, GlaconCoreOutput, GlaconRecord, OutputContext};
use helium_iceberg::{BatchedWriter, BatchedWriterConfig};
use std::marker::PhantomData;
use tokio::task::JoinHandle;

/// A [`GlaconCoreOutput`] writing one Iceberg table, whose rows a
/// [`GlaconOutput`] makes from records. Rows go through a
/// `helium_iceberg::BatchedWriter`: spooled to disk, then committed in
/// batches.
///
/// ```ignore
/// route.output("verified", IcebergOutput::new(VerifiedTable, verified_settings));
/// ```
pub struct IcebergOutput<O: GlaconOutput<R>, R> {
    output: O,
    settings: OutputSettings,
    /// Set by `init`.
    writing: Option<Writing<O::Row>>,
    _record: PhantomData<fn(&R)>,
}

/// The table's batched writer and its task.
struct Writing<Row> {
    ctx: OutputContext,
    writer: BatchedWriter<Row>,
    stop: triggered::Trigger,
    task: JoinHandle<helium_iceberg::Result<()>>,
}

impl<O: GlaconOutput<R>, R> IcebergOutput<O, R> {
    pub fn new(output: O, settings: OutputSettings) -> Self {
        Self {
            output,
            settings,
            writing: None,
            _record: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<O, R> GlaconCoreOutput<R> for IcebergOutput<O, R>
where
    O: GlaconOutput<R>,
    R: GlaconRecord,
{
    /// Connect the catalog, create the table if missing and start its
    /// writer, spooling to `<spool_dir>/<route>/<output>`.
    async fn init(&mut self, ctx: &OutputContext) -> Result<(), BoxError> {
        let catalog = self.settings.catalog.connect().await?;
        let definition = self.output.table()?;
        catalog
            .create_namespace_if_not_exists(definition.namespace())
            .await?;
        let table = catalog
            .create_table_if_not_exists::<O::Row>(definition)
            .await?;

        let spool_dir = self.settings.spool_dir.join(ctx.route()).join(ctx.output());
        let config = BatchedWriterConfig::new(spool_dir)
            .with_max_batch_size(self.settings.max_batch_rows)
            .with_batch_timeout(self.settings.batch_timeout);
        let (writer, task) = BatchedWriter::new(table, config);
        let (stop, stopped) = triggered::trigger();

        self.writing = Some(Writing {
            ctx: ctx.clone(),
            writer,
            stop,
            task: tokio::spawn(task.run(stopped)),
        });
        Ok(())
    }

    /// Turn the records into rows and queue them; returns once they are in
    /// the spool.
    async fn handle_data(&mut self, records: &[&R]) -> Result<(), BoxError> {
        let writing = self.writing.as_ref().ok_or("not initialized")?;
        let mut rows = Vec::new();
        for record in records {
            self.output.to_rows(record, &mut rows);
        }
        let count = rows.len() as u64;
        writing.writer.queue_all(rows).await?;
        metrics::output_counter(
            metrics::OUTPUT_ROWS,
            writing.ctx.route(),
            writing.ctx.output(),
            count,
        );
        Ok(())
    }

    /// Commit what is spooled and stop the writer.
    async fn terminate(&mut self) -> Result<(), BoxError> {
        let Some(writing) = self.writing.take() else {
            return Ok(());
        };
        let Writing {
            writer, stop, task, ..
        } = writing;
        drop(writer);
        stop.trigger();
        Ok(task.await??)
    }
}
