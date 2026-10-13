use super::OutputId;
use crate::Result;
use std::collections::HashMap;

/// A route's outputs by name, handed to the factory given to
/// [`RouteBuilder::middleware_with`](crate::RouteBuilder::middleware_with).
#[derive(Debug, Clone)]
pub struct OutputIndex {
    route: String,
    ids: HashMap<String, OutputId>,
}

impl OutputIndex {
    pub(crate) fn new<'a>(route: &str, names: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            route: route.to_string(),
            ids: names
                .into_iter()
                .enumerate()
                .map(|(i, name)| (name.to_string(), OutputId(i as u8)))
                .collect(),
        }
    }

    /// The id of output `name`; an unknown name is an error, so a typo fails
    /// the route's build.
    pub fn id(&self, name: &str) -> Result<OutputId> {
        self.ids.get(name).copied().ok_or_else(|| {
            crate::Error::invalid_route(
                &self.route,
                format!("middleware refers to unknown output {name:?}"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_names_in_order() -> crate::Result<()> {
        let index = OutputIndex::new("r", ["lake", "archive"]);
        assert_eq!(index.id("lake")?, OutputId(0));
        assert_eq!(index.id("archive")?, OutputId(1));
        let err = index.id("nope").unwrap_err().to_string();
        assert!(err.contains("unknown output \"nope\""), "{err}");
        Ok(())
    }
}
