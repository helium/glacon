use crate::{Error, GlaconRecord, Result, Route, tasks::NamedTask};
use std::{collections::HashSet, path::PathBuf};
use task_manager::TaskManager;

/// Every route of an app, each running as its own task. If any route fails,
/// the whole process stops.
#[derive(Default)]
pub struct Glacon {
    routes: Vec<(String, PathBuf, NamedTask)>,
}

impl Glacon {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn route<R: GlaconRecord>(mut self, route: Route<R>) -> Self {
        let name = route.name().to_string();
        let state_dir = route.settings().state_dir.clone();

        self.routes.push((
            name.clone(),
            state_dir,
            NamedTask::new(format!("route {name}"), route),
        ));

        self
    }

    /// The TaskManager running every route. Use `.start()` on it for signal
    /// handling (SIGTERM / Ctrl+C), or `ManagedTask::start_task` to drive
    /// shutdown yourself.
    pub fn into_task_manager(self) -> Result<TaskManager> {
        let mut names = HashSet::new();
        let mut dirs = HashSet::new();

        for (name, dir, _) in &self.routes {
            if !names.insert(name.clone()) {
                return Err(Error::invalid_route(
                    name,
                    "another route has the same name",
                ));
            }

            if !dirs.insert(dir.clone()) {
                return Err(Error::invalid_route(
                    name,
                    format!("state_dir {} is used by another route", dir.display()),
                ));
            }
        }

        Ok(crate::tasks::task_manager(
            self.routes.into_iter().map(|(_, _, task)| task),
        ))
    }

    /// Run every route until they all finish or SIGTERM / Ctrl+C.
    pub async fn run(self) -> Result {
        self.into_task_manager()?
            .start()
            .await
            .map_err(|source| Error::Task {
                route: "*".to_string(),
                source,
            })
    }
}
