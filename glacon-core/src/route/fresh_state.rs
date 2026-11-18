/// What to do when a route starts with an empty ledger (`state.db`) but one
/// of its outputs already holds data (a lost volume, or a route pointed at
/// tables another deployment wrote).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FreshState {
    /// Refuse to start (the default): every input would start over from its
    /// beginning, most likely duplicating data.
    #[default]
    Refuse,
    /// Start anyway, each input from its beginning.
    Proceed,
}
