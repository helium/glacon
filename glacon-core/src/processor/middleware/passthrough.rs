use super::GlaconCoreMiddleware;
use crate::{BatchId, BoxError, GlaconRecord, RecordResult};
use std::convert::Infallible;

/// Default middleware: every record passes unchanged, as `Ok`; none fails.
#[derive(Debug, Default, Clone, Copy)]
pub struct Passthrough;

#[async_trait::async_trait]
impl<R: GlaconRecord> GlaconCoreMiddleware<R, R> for Passthrough {
    type Error = Infallible;

    async fn handle_data(
        &mut self,
        _batch: &BatchId,
        records: Vec<R>,
    ) -> Result<Vec<RecordResult<R, Infallible>>, BoxError> {
        Ok(records.into_iter().map(Ok).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn passes_records_unchanged() -> Result<(), BoxError> {
        let batch = BatchId {
            input: "in".into(),
            key: "k".to_string(),
            timestamp: chrono::Utc::now(),
        };
        assert_eq!(
            Passthrough.handle_data(&batch, vec![1, 2]).await?,
            [Ok(1), Ok(2)]
        );
        Ok(())
    }
}
