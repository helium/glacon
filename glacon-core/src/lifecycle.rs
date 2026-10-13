//! The route's view of `init` and `terminate`.
//!
//! Inputs, middlewares and outputs are three different traits, each with its
//! own `init` / `terminate` signature and error variant. Wrapped with their
//! context in a [`RouteInput`](crate::input::RouteInput),
//! [`RouteMiddleware`](crate::processor::RouteMiddleware) or
//! [`RouteOutput`](crate::output::RouteOutput), they all implement
//! [`Lifecycle`], so a route starts and undoes any of them the same way.

use crate::Result;
use futures::future;

/// `init` and `terminate` of one of the route's inputs, middlewares or
/// outputs, with errors already naming it.
#[async_trait::async_trait]
pub(crate) trait Lifecycle: Send {
    /// How it is named in logs (e.g. "input us").
    fn describe(&self) -> String;
    async fn init(&mut self) -> Result;
    async fn terminate(&mut self) -> Result;
}

/// Initialize all of them at once. If one fails, those that succeeded are
/// terminated and the first error is returned.
pub(crate) async fn init_all<L: Lifecycle>(mut all: Vec<L>) -> Result<Vec<L>> {
    let results = future::join_all(all.iter_mut().map(|one| one.init())).await;
    if results.iter().all(Result::is_ok) {
        return Ok(all);
    }

    let mut failure = None;
    let mut started = Vec::new();
    for (one, result) in all.into_iter().zip(results) {
        match result {
            Ok(()) => started.push(one),
            Err(error) if failure.is_none() => failure = Some(error),
            Err(error) => tracing::error!(%error, "another failed to initialize"),
        }
    }
    terminate_all(started).await;
    Err(failure.expect("one init failed"))
}

/// Terminate all of them at once, logging failures. Used to undo a start
/// that failed half-way.
pub(crate) async fn terminate_all<L: Lifecycle>(mut all: Vec<L>) {
    let results = future::join_all(all.iter_mut().map(|one| one.terminate())).await;
    for (one, result) in all.iter().zip(results) {
        if let Err(error) = result {
            tracing::error!(what = %one.describe(), %error, "failed to terminate");
        }
    }
}
