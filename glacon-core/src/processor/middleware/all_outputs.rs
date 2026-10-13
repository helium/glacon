use super::{Destination, GlaconCoreMiddleware};
use crate::{BoxError, GlaconRecord};

/// Default middleware: every record goes to every output.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllOutputs;

#[async_trait::async_trait]
impl<R: GlaconRecord> GlaconCoreMiddleware<R> for AllOutputs {
    async fn handle_data(&mut self, records: &[R]) -> Result<Vec<Destination>, BoxError> {
        Ok(vec![Destination::All; records.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sends_every_record_everywhere() -> Result<(), BoxError> {
        let destinations = AllOutputs.handle_data(&[1, 2]).await?;
        assert_eq!(destinations, [Destination::All, Destination::All]);
        Ok(())
    }
}
