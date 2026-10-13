use crate::{Error, Result};
use futures::FutureExt;
use std::{
    any::Any,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Mutex, PoisonError},
};
use task_manager::{ManagedTask, TaskFuture};

/// One of a route's tasks, for its TaskManager.
///
/// `ManagedTask` requires `Sync`, but what a task owns (an input, a
/// middleware or an output) only needs `Send`: it sits behind a mutex, taken
/// once at start. A panic becomes an [`Error::Panic`] naming the task.
pub(crate) struct RouteTask<F> {
    route: String,
    name: String,
    run: Mutex<F>,
}

impl<F, Fut> RouteTask<F>
where
    F: FnOnce(triggered::Listener) -> Fut + Send + 'static,
    Fut: Future<Output = Result> + Send + 'static,
{
    pub fn new(route: &str, name: String, run: F) -> Self {
        Self {
            route: route.to_string(),
            name,
            run: Mutex::new(run),
        }
    }
}

impl<F, Fut> ManagedTask for RouteTask<F>
where
    F: FnOnce(triggered::Listener) -> Fut + Send + 'static,
    Fut: Future<Output = Result> + Send + 'static,
{
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        let RouteTask { route, name, run } = *self;
        let run = run.into_inner().unwrap_or_else(PoisonError::into_inner);
        task_manager::spawn(async move {
            AssertUnwindSafe(run(shutdown))
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| {
                    Err(Error::Panic {
                        route,
                        task: name,
                        message: panic_message(&*panic),
                    })
                })
        })
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic".to_string())
}
