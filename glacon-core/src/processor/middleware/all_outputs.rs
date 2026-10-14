use super::{Destination, GlaconCoreMiddleware};
use crate::{BatchId, BoxError, GlaconRecord};

/// Default middleware: every record goes to every output.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllOutputs;

#[async_trait::async_trait]
impl<R: GlaconRecord> GlaconCoreMiddleware<R> for AllOutputs {
    async fn handle_data(
        &mut self,
        _batch: &BatchId,
        records: &[R],
    ) -> Result<Vec<Destination>, BoxError> {
        Ok(vec![Destination::All; records.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sends_every_record_everywhere() -> Result<(), BoxError> {
        let batch = BatchId {
            input: "in".into(),
            key: "k".to_string(),
            timestamp: chrono::Utc::now(),
        };
        let destinations = AllOutputs.handle_data(&batch, &[1, 2]).await?;
        assert_eq!(destinations, [Destination::All, Destination::All]);
        Ok(())
    }
}
