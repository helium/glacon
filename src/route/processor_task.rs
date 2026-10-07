use super::Processor;
use crate::{GlaconRecord, input::DecodedFiles};
use std::sync::Mutex;
use task_manager::{ManagedTask, TaskFuture};

/// The processor and the merged stream of all inputs' files, as a task. The
/// stream is not `Sync`, which `ManagedTask` requires, hence the mutex; it is
/// only ever taken once.
pub(crate) struct ProcessorTask<R> {
    pub processor: Processor<R>,
    pub files: Mutex<DecodedFiles<R>>,
}

impl<R: GlaconRecord> ManagedTask for ProcessorTask<R> {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        let ProcessorTask { processor, files } = *self;
        let files = files.into_inner().unwrap_or_else(|e| e.into_inner());
        task_manager::spawn(processor.run(files, shutdown))
    }
}
