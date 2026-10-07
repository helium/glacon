use super::{FreshState, Route};
use crate::{
    AllOutputs, Error, GlaconInput, GlaconMiddleware, GlaconOutput, GlaconRecord, InputSettings,
    MAX_OUTPUTS, OutputIndex, OutputSettings, Result, RouteSettings,
    input::{InputEntry, InputSpec},
    output::{OutputEntry, OutputSpec},
};
use std::{collections::HashSet, marker::PhantomData};

/// Creates the route's middleware once its outputs are known.
type MiddlewareFactory<R> =
    Box<dyn FnOnce(&OutputIndex) -> Result<Box<dyn GlaconMiddleware<R>>> + Send>;

impl<R: GlaconRecord> Route<R> {
    pub fn builder(name: impl Into<String>, settings: RouteSettings) -> RouteBuilder<R> {
        RouteBuilder {
            name: name.into(),
            settings: Some(settings),
            inputs: Vec::new(),
            middleware: None,
            outputs: Vec::new(),
            fresh_state: FreshState::default(),
        }
    }
}

/// Builds a [`Route`]; [`RouteBuilder::build`] validates it.
///
/// Every method chains. A middleware that selects outputs is created with
/// [`RouteBuilder::middleware_with`], which looks the outputs up by name:
///
/// ```ignore
/// let route = Route::builder("events", settings)
///     .input("us", EventV1Input, us)
///     .input("eu", EventV2Input, eu)
///     .output("lake", EventsTable, lake_settings)
///     .output("values", ValuesTable, values_settings)
///     .middleware_with(|outputs| {
///         Ok(ByKind {
///             lake: outputs.id("lake")?,
///             values: outputs.id("values")?,
///         })
///     })
///     .build()?;
/// ```
pub struct RouteBuilder<R> {
    name: String,
    /// Taken by `build`; `None` once the route was built.
    settings: Option<RouteSettings>,
    inputs: Vec<Box<dyn InputSpec<R>>>,
    middleware: Option<MiddlewareFactory<R>>,
    outputs: Vec<Box<dyn OutputSpec<R>>>,
    fresh_state: FreshState,
}

impl<R: GlaconRecord> RouteBuilder<R> {
    /// Add an input reading `settings.bucket`. `name` identifies it in the
    /// state database and in metrics, so keep it stable.
    pub fn input<I>(
        &mut self,
        name: impl Into<String>,
        input: I,
        settings: InputSettings,
    ) -> &mut Self
    where
        I: GlaconInput<Record = R>,
    {
        self.inputs.push(Box::new(InputEntry {
            name: name.into(),
            input,
            settings,
        }));
        self
    }

    /// Add an output. `name` identifies it to the middleware (see
    /// [`RouteBuilder::middleware_with`]), in the state database and in
    /// metrics, so keep it stable.
    pub fn output<O>(
        &mut self,
        name: impl Into<String>,
        output: O,
        settings: OutputSettings,
    ) -> &mut Self
    where
        O: GlaconOutput<R>,
    {
        self.outputs.push(Box::new(OutputEntry {
            name: name.into(),
            output,
            settings,
            _record: PhantomData,
        }));
        self
    }

    /// Set a middleware that needs no output ids (default: [`AllOutputs`]).
    pub fn middleware<M: GlaconMiddleware<R>>(&mut self, middleware: M) -> &mut Self {
        self.middleware = Some(Box::new(move |_| Ok(Box::new(middleware))));
        self
    }

    /// Set a middleware built from the route's outputs: `factory` runs during
    /// [`RouteBuilder::build`] and looks outputs up with [`OutputIndex::id`].
    /// An unknown output name fails the build.
    pub fn middleware_with<M, F>(&mut self, factory: F) -> &mut Self
    where
        M: GlaconMiddleware<R>,
        F: FnOnce(&OutputIndex) -> Result<M> + Send + 'static,
    {
        self.middleware = Some(Box::new(move |outputs| {
            Ok(Box::new(factory(outputs)?) as Box<dyn GlaconMiddleware<R>>)
        }));
        self
    }

    /// What to do when the state is empty but an output table has data
    /// (default: [`FreshState::Refuse`]).
    pub fn fresh_state(&mut self, policy: FreshState) -> &mut Self {
        self.fresh_state = policy;
        self
    }

    /// Validate and build the route. Takes the builder's contents, so it can
    /// end a chain; building the same builder twice is an error.
    pub fn build(&mut self) -> Result<Route<R>> {
        let name = std::mem::take(&mut self.name);
        let invalid = |reason: String| Error::invalid_route(&name, reason);

        let settings = self
            .settings
            .take()
            .ok_or_else(|| invalid("route was already built".into()))?;
        let inputs = std::mem::take(&mut self.inputs);
        let outputs = std::mem::take(&mut self.outputs);
        let middleware = self.middleware.take();

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
        check_names("input", inputs.iter().map(|i| i.name())).map_err(invalid)?;
        check_names("output", outputs.iter().map(|o| o.name())).map_err(invalid)?;

        let middleware = match middleware {
            Some(factory) => factory(&OutputIndex::new(&name, outputs.iter().map(|o| o.name())))?,
            None => Box::new(AllOutputs),
        };

        Ok(Route {
            name,
            settings,
            inputs,
            middleware,
            outputs,
            fresh_state: self.fresh_state,
        })
    }
}

/// Names end up in the state database, spool paths and metric labels.
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
    use crate::{Destination, FileMeta, OutputId};
    use chrono::{DateTime, Utc};

    struct Rec;
    impl GlaconRecord for Rec {}

    #[derive(Clone, PartialEq, prost::Message)]
    struct Msg {}

    struct In;
    impl GlaconInput for In {
        type Proto = Msg;
        type Record = Rec;
        fn prefix(&self) -> &str {
            "msg"
        }
        fn decode(&self, _: Msg, _: FileMeta<'_>) -> Option<Rec> {
            Some(Rec)
        }
    }

    struct Out;
    impl GlaconOutput<Rec> for Out {
        type Row = ();
        fn table(&self) -> helium_iceberg::Result<helium_iceberg::TableDefinition> {
            unreachable!("not connected in these tests")
        }
        fn to_rows(&self, _: &Rec, _: &mut Vec<()>) {}
    }

    /// Sends every record to one output.
    struct Only(OutputId);
    impl GlaconMiddleware<Rec> for Only {
        fn route(&self, _: &Rec) -> Destination {
            Destination::Only(self.0.into())
        }
    }

    fn input() -> InputSettings {
        InputSettings::new(Default::default(), DateTime::<Utc>::UNIX_EPOCH)
    }

    fn output() -> OutputSettings {
        OutputSettings::new(helium_iceberg::Settings {
            catalog_uri: String::new(),
            catalog_name: String::new(),
            warehouse: None,
            auth: Default::default(),
            s3: Default::default(),
            properties: Default::default(),
        })
    }

    fn test_route_builder() -> RouteBuilder<Rec> {
        Route::builder("route", RouteSettings::new("/tmp/glacon-unit"))
    }

    fn err(route_builder: &mut RouteBuilder<Rec>) -> String {
        match route_builder.build() {
            Ok(_) => panic!("expected an invalid route"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn valid_route_builds_in_one_chain() -> Result<()> {
        let route = test_route_builder()
            .input("a", In, input())
            .input("b", In, input())
            .output("x", Out, output())
            .output("y", Out, output())
            .middleware_with(|outputs| Ok(Only(outputs.id("y")?)))
            .build()?;
        assert_eq!(route.name(), "route");
        Ok(())
    }

    #[test]
    fn middleware_with_rejects_unknown_outputs() {
        let e = err(test_route_builder()
            .input("a", In, input())
            .output("x", Out, output())
            .middleware_with(|outputs| Ok(Only(outputs.id("typo")?))));
        assert!(e.contains("unknown output \"typo\""), "{e}");
    }

    #[test]
    fn a_builder_builds_once() -> Result<()> {
        let mut route_builder = test_route_builder();
        route_builder
            .input("a", In, input())
            .output("x", Out, output());
        route_builder.build()?;
        assert!(err(&mut route_builder).contains("already built"));
        Ok(())
    }

    #[test]
    fn needs_inputs_and_outputs() {
        let e = err(test_route_builder().output("x", Out, output()));
        assert!(e.contains("at least one input"), "{e}");
        let e = err(test_route_builder().input("a", In, input()));
        assert!(e.contains("at least one output"), "{e}");
    }

    #[test]
    fn names_must_be_unique_and_safe() {
        let e = err(test_route_builder()
            .input("a", In, input())
            .input("a", In, input())
            .output("x", Out, output()));
        assert!(e.contains("duplicate input name \"a\""), "{e}");

        let e = err(test_route_builder()
            .input("a", In, input())
            .output("x", Out, output())
            .output("x", Out, output()));
        assert!(e.contains("duplicate output name \"x\""), "{e}");

        let e = err(test_route_builder()
            .input("A/b", In, input())
            .output("x", Out, output()));
        assert!(e.contains("must match"), "{e}");

        let e = err(Route::builder("", RouteSettings::new("/tmp/x"))
            .input("a", In, input())
            .output("x", Out, output()));
        assert!(e.contains("route name"), "{e}");
    }

    #[test]
    fn caps_the_number_of_outputs() {
        let mut route_builder = test_route_builder();
        route_builder.input("a", In, input());
        for i in 0..=MAX_OUTPUTS {
            route_builder.output(format!("o{i}"), Out, output());
        }
        assert!(err(&mut route_builder).contains("at most 64 outputs"));
    }
}
