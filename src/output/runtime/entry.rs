use super::{ConnectedOutput, OutputSpec, batched_sink::BatchedSink};
use crate::{GlaconOutput, OutputSettings, Result, tasks::NamedTask};
use helium_iceberg::{BatchedWriter, BatchedWriterConfig};
use std::{marker::PhantomData, path::Path};

/// An output as added to a route: its name, implementation and settings.
pub(crate) struct OutputEntry<R, O> {
    pub name: String,
    pub output: O,
    pub settings: OutputSettings,
    pub _record: PhantomData<fn(&R)>,
}

#[async_trait::async_trait]
impl<R, O> OutputSpec<R> for OutputEntry<R, O>
where
    R: Send + Sync + 'static,
    O: GlaconOutput<R>,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn connect(self: Box<Self>, spool_dir: &Path) -> Result<ConnectedOutput<R>> {
        let OutputEntry {
            name,
            output,
            settings,
            ..
        } = *self;

        let catalog = settings.catalog.connect().await?;
        let definition = output.table()?;
        let namespace = definition.namespace().to_string();
        let table_name = definition.name().to_string();
        let table = format!("{namespace}.{table_name}");

        catalog.create_namespace_if_not_exists(&namespace).await?;
        let had_data = catalog.table_exists(&namespace, &table_name).await?
            && catalog
                .load_table(&namespace, &table_name)
                .await?
                .metadata()
                .current_snapshot()
                .is_some();
        let iceberg_table = catalog
            .create_table_if_not_exists::<O::Row>(definition)
            .await?;

        let config = BatchedWriterConfig::new(spool_dir.join(&name))
            .with_max_batch_size(settings.max_batch_rows)
            .with_batch_timeout(settings.batch_timeout);
        let (writer, writer_task) = BatchedWriter::new(iceberg_table, config);

        Ok(ConnectedOutput {
            writer: NamedTask::new(format!("writer {name} ({table})"), writer_task),
            sink: Box::new(BatchedSink::new(name, output, writer)),
            table,
            had_data,
        })
    }
}
