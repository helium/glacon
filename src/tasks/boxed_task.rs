use task_manager::{ManagedTask, TaskFuture};

/// `Box<dyn ManagedTask>` as a `ManagedTask`, so tasks of different types can
/// be registered with one TaskManager.
pub(super) struct BoxedTask(pub(super) Box<dyn ManagedTask>);

impl ManagedTask for BoxedTask {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        self.0.start_task(shutdown)
    }
}
