/// The Rust value a route carries from its inputs to its outputs.
///
/// Every input of a route decodes into the same record type, the middleware
/// sees it, and each output turns it into its own rows.
pub trait GlaconRecord: Send + Sync + 'static {
    /// Key used to drop records the route has already seen, across all of its
    /// inputs. `None` (the default) means the record has no key: it is never
    /// treated as a duplicate and never remembered.
    fn dedup_key(&self) -> Option<&str> {
        None
    }
}
