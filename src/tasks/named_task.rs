use task_manager::ManagedTask;

/// A boxed task plus the name the TaskManager logs it under.
pub(crate) struct NamedTask {
    pub(super) name: String,
    pub(super) task: Box<dyn ManagedTask>,
}

impl NamedTask {
    pub(crate) fn new(name: impl Into<String>, task: impl ManagedTask + 'static) -> Self {
        Self {
            name: name.into(),
            task: Box::new(task),
        }
    }
}
