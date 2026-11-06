use super::GlaconCoreMiddleware;
use crate::{BatchId, BoxError, GlaconRecord};

/// Default middleware: records pass unchanged.
#[derive(Debug, Default, Clone, Copy)]
pub struct Passthrough;

#[async_trait::async_trait]
impl<R: GlaconRecord> GlaconCoreMiddleware<R, R> for Passthrough {
    async fn handle_data(&mut self, _batch: &BatchId, records: Vec<R>) -> Result<Vec<R>, BoxError> {
        Ok(records)
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
        assert_eq!(Passthrough.handle_data(&batch, vec![1, 2]).await?, [1, 2]);
        Ok(())
    }
}
