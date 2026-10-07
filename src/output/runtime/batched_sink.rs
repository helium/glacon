use super::OutputSink;
use crate::{GlaconOutput, Result};
use helium_iceberg::BatchedWriter;
use std::marker::PhantomData;

/// [`OutputSink`] that stages an output's rows in memory and queues them to
/// the table's `BatchedWriter` on flush.
pub(crate) struct BatchedSink<R, O: GlaconOutput<R>> {
    name: String,
    output: O,
    writer: BatchedWriter<O::Row>,
    rows: Vec<O::Row>,
    records: u64,
    _record: PhantomData<fn(&R)>,
}

impl<R, O: GlaconOutput<R>> BatchedSink<R, O> {
    pub fn new(name: String, output: O, writer: BatchedWriter<O::Row>) -> Self {
        Self {
            name,
            output,
            writer,
            rows: Vec::new(),
            records: 0,
            _record: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<R, O> OutputSink<R> for BatchedSink<R, O>
where
    R: Send + Sync + 'static,
    O: GlaconOutput<R>,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn stage(&mut self, record: &R) {
        self.output.to_rows(record, &mut self.rows);
        self.records += 1;
    }

    async fn flush(&mut self) -> Result<(u64, u64)> {
        let records = std::mem::take(&mut self.records);
        let rows = std::mem::take(&mut self.rows);
        let row_count = rows.len() as u64;
        self.writer.queue_all(rows).await?;
        Ok((records, row_count))
    }

    fn discard(&mut self) {
        self.rows.clear();
        self.records = 0;
    }
}
