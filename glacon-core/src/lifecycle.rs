//! The route's view of `init` and `terminate`.
//!
//! Inputs, middlewares and outputs are three different traits, each with its
//! own `init` / `terminate` signature and error variant. Wrapped with their
//! context, they all implement [`Lifecycle`] and [`Terminate`], so a route
//! starts and undoes any of them the same way:
//!
//! | Before `init` | After `init` |
//! |---|---|
//! | [`PendingInput`](crate::input::PendingInput) | [`RouteInput`](crate::input::RouteInput) (the input with its state) |
//! | [`RouteMiddleware`](crate::processor::RouteMiddleware) | the same |
//! | [`PendingOutput`](crate::output::PendingOutput) | [`RouteOutput`](crate::output::RouteOutput) (the output with its state) |

use crate::Result;
use futures::future;
use task_manager::ManagedTask;

/// One of the route's inputs, middlewares or outputs once initialized, with
/// the background task its `init` returned, if any.
pub(crate) type Initialized<T> = (T, Option<Box<dyn ManagedTask>>);

/// `init` of one of the route's inputs, middlewares or outputs, with errors
/// already naming it.
#[async_trait::async_trait]
pub(crate) trait Lifecycle: Send + Sized {
    /// What `init` turns it into.
    type Started: Terminate;

    async fn init(self) -> Result<Initialized<Self::Started>>;
}

/// Undo `init` once it succeeded. Inputs have nothing to undo.
#[async_trait::async_trait]
pub(crate) trait Terminate: Send {
    async fn terminate(&mut self) -> Result {
        Ok(())
    }
}

/// Initialize all of them at once; returns each started, with its background
/// task. If one fails, the tasks of the others are dropped (never started),
/// those that succeeded are terminated, and the first error is returned.
pub(crate) async fn init_all<L: Lifecycle>(all: Vec<L>) -> Result<Vec<Initialized<L::Started>>> {
    let results = future::join_all(all.into_iter().map(|one| one.init())).await;

    let mut failure = None;
    let mut started = Vec::new();
    for result in results {
        match result {
            Ok(one) => started.push(one),
            Err(error) if failure.is_none() => failure = Some(error),
            Err(error) => tracing::error!(%error, "another failed to initialize"),
        }
    }
    match failure {
        None => Ok(started),
        Some(error) => {
            terminate_all(started.into_iter().map(|(one, _task)| one).collect()).await;
            Err(error)
        }
    }
}

/// Terminate all of them at once, logging failures (their errors name
/// them). Used to undo a start that failed half-way.
pub(crate) async fn terminate_all<T: Terminate>(mut all: Vec<T>) {
    let results = future::join_all(all.iter_mut().map(|one| one.terminate())).await;
    for error in results.into_iter().filter_map(Result::err) {
        tracing::error!(%error, "failed to terminate");
    }
}
