use super::OutputId;

/// Maximum number of outputs per route (the size of an [`OutputSet`]).
pub const MAX_OUTPUTS: usize = 64;

/// A set of outputs, cheap to build and copy: bit `i` is output `i`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutputSet(u64);

impl OutputSet {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn with(mut self, id: OutputId) -> Self {
        self.insert(id);
        self
    }

    /// Add `id`. Ids past [`MAX_OUTPUTS`] are ignored (a route with that
    /// many outputs fails to build).
    pub fn insert(&mut self, id: OutputId) {
        self.0 |= bit(id);
    }

    pub fn contains(&self, id: OutputId) -> bool {
        self.0 & bit(id) != 0
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

fn bit(id: OutputId) -> u64 {
    1u64.checked_shl(u32::from(id.0)).unwrap_or(0)
}

impl From<OutputId> for OutputSet {
    fn from(id: OutputId) -> Self {
        Self::new().with(id)
    }
}

impl FromIterator<OutputId> for OutputSet {
    fn from_iter<I: IntoIterator<Item = OutputId>>(iter: I) -> Self {
        iter.into_iter().fold(Self::new(), Self::with)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_members() {
        let set = OutputSet::new().with(OutputId(0)).with(OutputId(63));
        assert!(set.contains(OutputId(0)));
        assert!(set.contains(OutputId(63)));
        assert!(!set.contains(OutputId(1)));
        assert!(!set.is_empty());
        assert!(OutputSet::new().is_empty());
        assert_eq!(set, [OutputId(63), OutputId(0)].into_iter().collect());
        assert_eq!(
            OutputSet::from(OutputId(3)),
            OutputSet::new().with(OutputId(3))
        );
    }

    #[test]
    fn ignores_ids_past_the_limit() {
        let set = OutputSet::new().with(OutputId(64)).with(OutputId(u8::MAX));
        assert!(set.is_empty());
        assert!(!set.contains(OutputId(64)));
    }
}
