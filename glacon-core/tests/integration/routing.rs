//! Records from N inputs, routed by each input's middleware to M outputs.

use crate::common::{self, End, Journal, MemoryInput, MemoryOutput, batch, event};
use glacon_core::{BoxError, Destination, Error, GlaconCoreMiddleware, Route, async_trait};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex, PoisonError},
};

#[tokio::test]
async fn routes_records_to_outputs() -> anyhow::Result<()> {
    let journal = Journal::default();
    let (left, right) = (
        MemoryOutput::new(common::LEFT, &journal),
        MemoryOutput::new(common::RIGHT, &journal),
    );
    let us = MemoryInput::new(
        "us",
        vec![
            batch(
                "us.1",
                vec![
                    event("l1", "left"),
                    event("b1", "both"),
                    event("d1", "drop"),
                ],
            ),
            batch("us.2", vec![event("r1", "right")]),
        ],
        End::Return,
        &journal,
    );
    let eu = MemoryInput::new(
        "eu",
        vec![batch(
            "eu.1",
            vec![event("l2", "left"), event("b2", "both")],
        )],
        End::Return,
        &journal,
    );

    let route = common::route("routing", vec![us, eu], &left, &right, &journal).build()?;
    common::run(route).await?;

    assert_eq!(left.rows(), ["b1", "b2", "l1", "l2"]);
    assert_eq!(right.rows(), ["b1", "b2", "r1"]);
    Ok(())
}

/// Returns no destination at all.
#[derive(Clone)]
struct Forgetful;

#[async_trait]
impl GlaconCoreMiddleware<common::Event> for Forgetful {
    async fn handle_data(&mut self, _: &[common::Event]) -> Result<Vec<Destination>, BoxError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn a_middleware_returns_one_destination_per_record() -> anyhow::Result<()> {
    let journal = Journal::default();
    let input = MemoryInput::new(
        "us",
        vec![batch("us.1", vec![event("a", "left")])],
        End::Return,
        &journal,
    );
    let route = Route::builder("forgetful")
        .input("us", input)
        .output(common::LEFT, MemoryOutput::new(common::LEFT, &journal))
        .middleware(Forgetful)
        .build()?;

    let err = common::run(route).await.expect_err("must fail");
    let err = common::route_error(&err);
    assert!(
        matches!(err, Error::Middleware { input, .. } if input == "us"),
        "{err}"
    );
    assert!(
        err.to_string().contains("0 destinations for 1 records"),
        "{err}"
    );
    Ok(())
}

/// Deduplication is the middleware's job: every processor gets its own
/// instance, sharing what was seen through an Arc.
#[derive(Clone, Default)]
struct Dedup {
    seen: Arc<Mutex<HashSet<String>>>,
}

#[async_trait]
impl GlaconCoreMiddleware<common::Event> for Dedup {
    async fn handle_data(
        &mut self,
        events: &[common::Event],
    ) -> Result<Vec<Destination>, BoxError> {
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(events
            .iter()
            .map(|e| {
                if seen.insert(e.id.clone()) {
                    Destination::All
                } else {
                    Destination::Drop
                }
            })
            .collect())
    }
}

#[tokio::test]
async fn deduplication_is_a_middleware() -> anyhow::Result<()> {
    let journal = Journal::default();
    let output = MemoryOutput::new(common::LEFT, &journal);
    let inputs = [("us", ["a", "b", "a"]), ("eu", ["b", "c", "c"])].map(|(name, ids)| {
        let events = ids.iter().map(|id| event(id, "both")).collect();
        MemoryInput::new(name, vec![batch(name, events)], End::Return, &journal)
    });

    let mut builder = Route::builder("dedup");
    for input in inputs {
        builder = builder.input(input.name, input);
    }
    let route = builder
        .output(common::LEFT, output.clone())
        .middleware(Dedup::default())
        .build()?;
    common::run(route).await?;

    assert_eq!(output.rows(), ["a", "b", "c"]);
    Ok(())
}
