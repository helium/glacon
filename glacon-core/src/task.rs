use crate::{BoxError, Error, Result};
use futures::FutureExt;
use std::{
    any::Any,
    future::Future,
    panic::{self, AssertUnwindSafe},
    sync::{Arc, Mutex, PoisonError},
};
use task_manager::{ManagedTask, TaskError, TaskFuture};

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

/// Which stage a background task belongs to, so its failure names it.
pub(crate) enum Owner {
    Input(Arc<str>),
    Output(Arc<str>),
}

impl Owner {
    /// How the task is named in the route's TaskManager and in errors.
    pub fn task_name(&self) -> String {
        match self {
            Owner::Input(input) => format!("input {input}: background"),
            Owner::Output(output) => format!("output {output}: background"),
        }
    }

    fn error(&self, route: &str, source: BoxError) -> Error {
        match self {
            Owner::Input(input) => Error::input(route, input, source),
            Owner::Output(output) => Error::output(route, output, source),
        }
    }
}

/// The background task an input or output returned from `init`, supervised
/// by its route. Its failure is reported as the stage's
/// ([`Error::Input`] / [`Error::Output`]); a panic becomes an
/// [`Error::Panic`] naming it.
pub(crate) struct BackgroundTask {
    route: Arc<str>,
    owner: Owner,
    task: Box<dyn ManagedTask>,
}

impl BackgroundTask {
    pub fn new(route: &Arc<str>, owner: Owner, task: Box<dyn ManagedTask>) -> Self {
        Self {
            route: route.clone(),
            owner,
            task,
        }
    }
}

impl ManagedTask for BackgroundTask {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        let BackgroundTask { route, owner, task } = *self;
        let started = panic::catch_unwind(AssertUnwindSafe(move || task.start_task(shutdown)));
        task_manager::spawn(async move {
            let panicked = |message: String| Error::Panic {
                route: route.to_string(),
                task: owner.task_name(),
                message,
            };
            let running = started.map_err(|panic| panicked(panic_message(&*panic)))?;
            match AssertUnwindSafe(running).catch_unwind().await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => {
                    match unwrap_task_error(error).downcast::<tokio::task::JoinError>() {
                        // A task it spawned panicked.
                        Ok(join) if join.is_panic() => {
                            Err(panicked(panic_message(&*join.into_panic())))
                        }
                        Ok(join) => Err(owner.error(&route, join)),
                        Err(source) => Err(owner.error(&route, source)),
                    }
                }
                Err(panic) => Err(panicked(panic_message(&*panic))),
            }
        })
    }
}

/// The error a task returned. A TaskManager run as a task wraps its tasks'
/// errors in another `TaskError`, so unwrap every layer.
pub(crate) fn unwrap_task_error(error: TaskError) -> BoxError {
    match error {
        TaskError::Task(inner) => match inner.downcast::<TaskError>() {
            Ok(nested) => unwrap_task_error(*nested),
            Err(inner) => inner,
        },
        other => other.into(),
    }
}
