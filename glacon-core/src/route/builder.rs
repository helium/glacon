use super::{Route, RouteParts};
use crate::{
    AllOutputs, Error, GlaconCoreInputInit, GlaconCoreMiddleware, GlaconCoreOutputInit,
    GlaconRecord, MAX_OUTPUTS, OutputIndex, Result,
};
use std::{collections::HashSet, sync::Mutex};

/// Creates a middleware once the route's outputs are known: once to validate
/// the route, then once per input's processor.
pub(crate) type MiddlewareFactory<R> =
    Box<dyn Fn(&OutputIndex) -> Result<Box<dyn GlaconCoreMiddleware<R>>> + Send>;

impl<R: GlaconRecord> Route<R> {
    pub fn builder(name: impl Into<String>) -> RouteBuilder<R> {
        RouteBuilder {
            name: name.into(),
            inputs: Vec::new(),
            middleware: None,
            outputs: Vec::new(),
        }
    }
}

/// Builds a [`Route`]; [`RouteBuilder::build`] validates it.
///
/// Every method takes the builder and returns it, so calls chain. A middleware that selects outputs is created with
/// [`RouteBuilder::middleware_with`], which looks the outputs up by name:
///
/// ```ignore
/// let route = Route::builder("speedtests")
///     .input("us", us_input)
///     .input("eu", eu_input)
///     .output("verified", verified_output)
///     .output("dropped", dropped_output)
///     .middleware_with(|outputs| {
///         Ok(VerifySpeedTest {
///             verified: outputs.id("verified")?,
///             dropped: outputs.id("dropped")?,
///         })
///     })
///     .build()?;
/// ```
pub struct RouteBuilder<R> {
    name: String,
    inputs: Vec<(String, Box<dyn GlaconCoreInputInit<R>>)>,
    middleware: Option<MiddlewareFactory<R>>,
    outputs: Vec<(String, Box<dyn GlaconCoreOutputInit<R>>)>,
}

impl<R: GlaconRecord> RouteBuilder<R> {
    /// Add an input. `name` identifies it in logs and metrics, and to the
    /// input itself through its context, so keep it stable.
    pub fn input(mut self, name: impl Into<String>, input: impl GlaconCoreInputInit<R>) -> Self {
        self.inputs.push((name.into(), Box::new(input)));
        self
    }

    /// Add an output. `name` identifies it to the middleware (see
    /// [`RouteBuilder::middleware_with`]), in logs and metrics, so keep it
    /// stable.
    pub fn output(mut self, name: impl Into<String>, output: impl GlaconCoreOutputInit<R>) -> Self {
        self.outputs.push((name.into(), Box::new(output)));
        self
    }

    /// Set a middleware that needs no output ids, cloned for each input's
    /// processor (default: [`AllOutputs`]). A route has one middleware:
    /// setting it again replaces it.
    pub fn middleware<M>(mut self, middleware: M) -> Self
    where
        M: GlaconCoreMiddleware<R> + Clone,
    {
        self.middleware = Some(Box::new(move |_| Ok(Box::new(middleware.clone()))));
        self
    }

    /// Set a middleware built from the route's outputs: `factory` looks them
    /// up with [`OutputIndex::id`]. It runs once during
    /// [`RouteBuilder::build`], where an unknown output name fails the build,
    /// then once per input when the route starts: every processor owns its
    /// middleware. A route has one middleware: setting it again replaces it.
    pub fn middleware_with<M, F>(mut self, factory: F) -> Self
    where
        M: GlaconCoreMiddleware<R>,
        F: Fn(&OutputIndex) -> Result<M> + Send + 'static,
    {
        self.middleware = Some(Box::new(move |outputs| {
            Ok(Box::new(factory(outputs)?) as Box<dyn GlaconCoreMiddleware<R>>)
        }));
        self
    }

    /// Validate and build the route.
    pub fn build(self) -> Result<Route<R>> {
        let RouteBuilder {
            name,
            inputs,
            middleware,
            outputs,
        } = self;
        let middleware = middleware.unwrap_or_else(|| Box::new(|_| Ok(Box::new(AllOutputs))));
        let invalid = |reason: String| Error::invalid_route(&name, reason);

        check_name("route", &name).map_err(invalid)?;
        if inputs.is_empty() {
            return Err(invalid("at least one input is required".into()));
        }
        if outputs.is_empty() {
            return Err(invalid("at least one output is required".into()));
        }
        if outputs.len() > MAX_OUTPUTS {
            return Err(invalid(format!(
                "at most {MAX_OUTPUTS} outputs are supported"
            )));
        }
        check_names("input", inputs.iter().map(|(name, _)| name.as_str())).map_err(invalid)?;
        check_names("output", outputs.iter().map(|(name, _)| name.as_str())).map_err(invalid)?;

        // Fails on an unknown output name; the processors get their own.
        let index = OutputIndex::new(&name, outputs.iter().map(|(name, _)| name.as_str()));
        middleware(&index)?;

        Ok(Route {
            name: name.as_str().into(),
            parts: Mutex::new(RouteParts {
                inputs,
                middleware,
                index,
                outputs,
            }),
        })
    }
}

/// Names end up in logs, metric labels and the names sinks derive from them.
fn check_name(kind: &str, name: &str) -> Result<(), String> {
    let valid = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if valid {
        Ok(())
    } else {
        Err(format!("{kind} name {name:?} must match [a-z0-9_-]+"))
    }
}

fn check_names<'a>(kind: &str, names: impl Iterator<Item = &'a str>) -> Result<(), String> {
    let mut seen = HashSet::new();
    for name in names {
        check_name(kind, name)?;
        if !seen.insert(name) {
            return Err(format!("duplicate {kind} name {name:?}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Batch, BoxError, Destination, GlaconCoreInput, GlaconCoreOutput, InitializedInput,
        InitializedOutput, InputContext, OutputContext, OutputId,
    };

    struct In;
    #[async_trait::async_trait]
    impl GlaconCoreInputInit<()> for In {
        async fn init(self: Box<Self>, _: &InputContext) -> Result<InitializedInput<()>, BoxError> {
            Ok(InitializedInput::new(*self))
        }
    }
    #[async_trait::async_trait]
    impl GlaconCoreInput<()> for In {
        async fn next_batch(&mut self) -> Result<Option<Batch<()>>, BoxError> {
            Ok(None)
        }
    }

    struct Out;
    #[async_trait::async_trait]
    impl GlaconCoreOutputInit<()> for Out {
        async fn init(
            self: Box<Self>,
            _: &OutputContext,
        ) -> Result<InitializedOutput<()>, BoxError> {
            Ok(InitializedOutput::new(*self))
        }
    }
    #[async_trait::async_trait]
    impl GlaconCoreOutput<()> for Out {
        async fn handle_data(&mut self, _: &[&()]) -> Result<(), BoxError> {
            Ok(())
        }
    }

    /// Sends every record to one output.
    struct Only(OutputId);
    #[async_trait::async_trait]
    impl GlaconCoreMiddleware<()> for Only {
        async fn handle_data(&mut self, records: &[()]) -> Result<Vec<Destination>, BoxError> {
            Ok(vec![Destination::Only(self.0.into()); records.len()])
        }
    }

    fn test_route_builder() -> RouteBuilder<()> {
        Route::builder("route")
    }

    fn err(route_builder: RouteBuilder<()>) -> String {
        match route_builder.build() {
            Ok(_) => panic!("expected an invalid route"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn valid_route_builds_in_one_chain() -> Result<()> {
        let route = test_route_builder()
            .input("a", In)
            .input("b", In)
            .output("x", Out)
            .output("y", Out)
            .middleware_with(|outputs| Ok(Only(outputs.id("y")?)))
            .build()?;
        assert_eq!(route.name(), "route");
        Ok(())
    }

    #[test]
    fn middleware_with_rejects_unknown_outputs() {
        let e = err(test_route_builder()
            .input("a", In)
            .output("x", Out)
            .middleware_with(|outputs| Ok(Only(outputs.id("typo")?))));
        assert!(e.contains("unknown output \"typo\""), "{e}");
    }

    #[test]
    fn needs_inputs_and_outputs() {
        let e = err(test_route_builder().output("x", Out));
        assert!(e.contains("at least one input"), "{e}");
        let e = err(test_route_builder().input("a", In));
        assert!(e.contains("at least one output"), "{e}");
    }

    #[test]
    fn names_must_be_unique_and_safe() {
        let e = err(test_route_builder()
            .input("a", In)
            .input("a", In)
            .output("x", Out));
        assert!(e.contains("duplicate input name \"a\""), "{e}");

        let e = err(test_route_builder()
            .input("a", In)
            .output("x", Out)
            .output("x", Out));
        assert!(e.contains("duplicate output name \"x\""), "{e}");

        let e = err(test_route_builder().input("A/b", In).output("x", Out));
        assert!(e.contains("must match"), "{e}");

        let e = err(Route::builder("").input("a", In).output("x", Out));
        assert!(e.contains("route name"), "{e}");
    }

    #[test]
    fn caps_the_number_of_outputs() {
        let mut route_builder = test_route_builder().input("a", In);
        for i in 0..=MAX_OUTPUTS {
            route_builder = route_builder.output(format!("o{i}"), Out);
        }
        assert!(err(route_builder).contains("at most 64 outputs"));
    }
}
