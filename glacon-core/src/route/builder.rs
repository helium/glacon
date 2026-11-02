use super::{FreshState, Route, RouteParts, RouteSettings};
use crate::{
    Error, GlaconCoreInput, GlaconCoreMiddleware, GlaconCoreOutput, GlaconRecord, Passthrough,
    Result, input::InitInput, output::InitOutput,
};
use std::{collections::HashSet, sync::Mutex};

/// Creates the middleware of each input's processor.
pub(crate) type MiddlewareFactory<In, Out> =
    Box<dyn Fn() -> Box<dyn GlaconCoreMiddleware<In, Out>> + Send>;

impl<In: GlaconRecord> Route<In> {
    /// Start building a route. `settings.state_dir` holds its ledger: give
    /// every route its own.
    pub fn builder(name: impl Into<String>, settings: RouteSettings) -> RouteBuilder<In> {
        RouteBuilder {
            name: name.into(),
            inputs: Vec::new(),
            settings,
            fresh_state: FreshState::default(),
        }
    }
}

/// Builds a [`Route`], in the order data flows: inputs, then optionally the
/// middleware, then outputs. Adding the middleware or the first output turns
/// it into a [`RouteOutputs`], which takes outputs and builds the route.
///
/// ```ignore
/// let route = Route::builder("speedtests", RouteSettings::new("/data/speedtests"))
///     .input("us", us_input)                   // produce SpeedTest
///     .input("eu", eu_input)
///     .middleware(VerifySpeedTest::default())  // SpeedTest -> VerifiedSpeedTest
///     .output("verified", verified_output)     // get VerifiedSpeedTest
///     .output("dropped", dropped_output)
///     .build()?;
/// ```
///
/// Without a middleware, outputs get the inputs' records unchanged
/// ([`Passthrough`]).
pub struct RouteBuilder<In> {
    name: String,
    inputs: Vec<(String, Box<dyn InitInput<In>>)>,
    settings: RouteSettings,
    fresh_state: FreshState,
}

impl<In: GlaconRecord> RouteBuilder<In> {
    /// Add an input. `name` identifies it in logs and metrics, and to the
    /// input itself through its context, so keep it stable.
    pub fn input(mut self, name: impl Into<String>, input: impl GlaconCoreInput<In>) -> Self {
        self.inputs.push((name.into(), Box::new(input)));
        self
    }

    /// What to do when the route starts with an empty ledger but an output
    /// already holds data (default: [`FreshState::Refuse`]).
    pub fn fresh_state(mut self, fresh_state: FreshState) -> Self {
        self.fresh_state = fresh_state;
        self
    }

    /// Set the route's middleware, turning the inputs' `In` records into the
    /// `Out` records outputs get. It is cloned for each input's processor:
    /// share state between the clones through an `Arc`, and create what
    /// can't be cloned (a connection) in its `init`.
    pub fn middleware<Out, M>(self, middleware: M) -> RouteOutputs<In, Out>
    where
        Out: GlaconRecord,
        M: GlaconCoreMiddleware<In, Out> + Clone,
    {
        RouteOutputs {
            route: self,
            middleware: Box::new(move || Box::new(middleware.clone())),
            outputs: Vec::new(),
        }
    }

    /// Add the first output of a route without a middleware: outputs get the
    /// inputs' records unchanged.
    pub fn output(
        self,
        name: impl Into<String>,
        output: impl GlaconCoreOutput<In>,
    ) -> RouteOutputs<In, In> {
        self.middleware(Passthrough).output(name, output)
    }
}

/// A [`RouteBuilder`] once its middleware is set: takes the outputs, then
/// [`RouteOutputs::build`] validates the route.
pub struct RouteOutputs<In, Out> {
    route: RouteBuilder<In>,
    middleware: MiddlewareFactory<In, Out>,
    outputs: Vec<(String, Box<dyn InitOutput<Out>>)>,
}

impl<In: GlaconRecord, Out: GlaconRecord> RouteOutputs<In, Out> {
    /// Add an output. It gets every record of every batch, and keeps what it
    /// wants. `name` identifies it in logs and metrics, and in the ledger, so
    /// keep it stable.
    pub fn output(mut self, name: impl Into<String>, output: impl GlaconCoreOutput<Out>) -> Self {
        self.outputs.push((name.into(), Box::new(output)));
        self
    }

    /// See [`RouteBuilder::fresh_state`].
    pub fn fresh_state(mut self, fresh_state: FreshState) -> Self {
        self.route.fresh_state = fresh_state;
        self
    }

    /// Validate and build the route.
    pub fn build(self) -> Result<Route<In, Out>> {
        let RouteOutputs {
            route:
                RouteBuilder {
                    name,
                    inputs,
                    settings,
                    fresh_state,
                },
            middleware,
            outputs,
        } = self;
        let invalid = |reason: String| Error::invalid_route(&name, reason);

        check_name("route", &name).map_err(invalid)?;
        if inputs.is_empty() {
            return Err(invalid("at least one input is required".into()));
        }
        if outputs.is_empty() {
            return Err(invalid("at least one output is required".into()));
        }
        check_names("input", inputs.iter().map(|(name, _)| name.as_str())).map_err(invalid)?;
        check_names("output", outputs.iter().map(|(name, _)| name.as_str())).map_err(invalid)?;

        Ok(Route {
            name: name.as_str().into(),
            parts: Mutex::new(RouteParts {
                inputs,
                middleware,
                outputs,
                settings,
                fresh_state,
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
        Batch, BatchId, BoxError, GlaconCoreInput, GlaconCoreOutput, InitializedInput,
        InitializedOutput, InputContext, OutputContext,
    };

    struct In;
    #[async_trait::async_trait]
    impl GlaconCoreInput<()> for In {
        type State = ();

        async fn init(&mut self, _: &InputContext) -> Result<InitializedInput<()>, BoxError> {
            Ok(InitializedInput::new(()))
        }

        async fn next_batch(&mut self, _: &mut ()) -> Result<Option<Batch<()>>, BoxError> {
            Ok(None)
        }
    }

    /// Writes any record type.
    struct Out;
    #[async_trait::async_trait]
    impl<R: GlaconRecord> GlaconCoreOutput<R> for Out {
        type State = ();

        async fn init(&mut self, _: &OutputContext) -> Result<InitializedOutput<()>, BoxError> {
            Ok(InitializedOutput::new(()))
        }

        async fn handle_data(&mut self, _: &mut (), _: &BatchId, _: &[R]) -> Result<(), BoxError> {
            Ok(())
        }
    }

    /// Turns every record into a number.
    #[derive(Clone)]
    struct Count;
    #[async_trait::async_trait]
    impl GlaconCoreMiddleware<(), u32> for Count {
        async fn handle_data(
            &mut self,
            _: &BatchId,
            records: Vec<()>,
        ) -> Result<Vec<u32>, BoxError> {
            Ok((0..records.len() as u32).collect())
        }
    }

    fn test_route_builder() -> RouteBuilder<()> {
        // Never started: the state dir is never created.
        Route::builder("route", RouteSettings::new("unused"))
    }

    fn err<Out: GlaconRecord>(route: RouteOutputs<(), Out>) -> String {
        match route.build() {
            Ok(_) => panic!("expected an invalid route"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn builds_with_a_middleware_changing_the_record_type() -> Result<()> {
        let route: Route<(), u32> = test_route_builder()
            .input("a", In)
            .input("b", In)
            .middleware(Count)
            .output("x", Out)
            .output("y", Out)
            .build()?;
        assert_eq!(route.name(), "route");
        Ok(())
    }

    #[test]
    fn builds_without_a_middleware() -> Result<()> {
        let route: Route<()> = test_route_builder()
            .input("a", In)
            .output("x", Out)
            .build()?;
        assert_eq!(route.name(), "route");
        Ok(())
    }

    #[test]
    fn needs_inputs_and_outputs() {
        let e = err(test_route_builder().output("x", Out));
        assert!(e.contains("at least one input"), "{e}");
        let e = err(test_route_builder().input("a", In).middleware(Count));
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

        let e = err(Route::builder("", RouteSettings::new("unused"))
            .input("a", In)
            .output("x", Out));
        assert!(e.contains("route name"), "{e}");
    }
}
