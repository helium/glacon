//! Small helpers to put tasks of different types in one TaskManager.

mod boxed_task;
mod named_task;

pub(crate) use named_task::NamedTask;

use boxed_task::BoxedTask;
use task_manager::TaskManager;

/// Build a TaskManager from tasks in start order. Tasks stop in reverse
/// order, so the first task stops last.
pub(crate) fn task_manager(tasks: impl IntoIterator<Item = NamedTask>) -> TaskManager {
    tasks
        .into_iter()
        .fold(TaskManager::builder(), |builder, t| {
            builder.add_named(t.name, BoxedTask(t.task))
        })
        .build()
}
