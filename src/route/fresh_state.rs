/// What to do when a route starts with an empty state database but one of its
/// output tables already has data (a lost volume, or a route pointed at
/// tables another deployment wrote).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FreshState {
    /// Refuse to start (the default): reprocessing from each input's
    /// `start_after` would most likely duplicate data.
    #[default]
    Refuse,
    /// Start anyway, from each input's `start_after`.
    Proceed,
}
