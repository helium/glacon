use super::{Destination, GlaconMiddleware};

/// Default middleware: every record goes to every output.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllOutputs;

impl<R> GlaconMiddleware<R> for AllOutputs {
    fn route(&self, _record: &R) -> Destination {
        Destination::All
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sends_every_record_everywhere() {
        assert_eq!(
            GlaconMiddleware::<()>::route(&AllOutputs, &()),
            Destination::All
        );
    }
}
