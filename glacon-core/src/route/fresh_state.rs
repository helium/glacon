use serde::{Deserialize, Serialize};

/// What to do when a route starts with an empty ledger (`state.db`) but one
/// of its outputs already holds data (a lost volume, or a route pointed at
/// tables another deployment wrote).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshState {
    /// Refuse to start (the default): every input would start over from its
    /// beginning, most likely duplicating data.
    #[default]
    Refuse,
    /// Start anyway, each input from its beginning.
    Proceed,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deployments set this by name (`fresh_state = "refuse"`): renaming a
    /// variant breaks their configuration.
    #[test]
    fn configured_by_snake_case_name() -> anyhow::Result<()> {
        for (name, state) in [
            ("refuse", FreshState::Refuse),
            ("proceed", FreshState::Proceed),
        ] {
            let quoted = format!("\"{name}\"");
            assert_eq!(serde_json::from_str::<FreshState>(&quoted)?, state);
            assert_eq!(serde_json::to_string(&state)?, quoted);
        }
        Ok(())
    }
}
