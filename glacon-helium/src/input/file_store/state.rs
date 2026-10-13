use ::file_store::{
    FileInfo,
    file_info_poller::{FileInfoPollerState, FileInfoPollerStateRecorder},
};
use chrono::{DateTime, Utc};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
};

/// Where a [`FileStoreInput`](super::FileStoreInput)'s poller keeps what it
/// read: its resume point, and the files to skip when they are listed again.
///
/// Implemented for:
/// - `sqlx::PgPool`: file_store's `files_processed` table, which the app
///   creates (file_store has no migration for it):
///   ```sql
///   CREATE TABLE files_processed (
///       process_name TEXT NOT NULL DEFAULT 'default',
///       file_name VARCHAR PRIMARY KEY,
///       file_type VARCHAR NOT NULL,
///       file_timestamp TIMESTAMPTZ NOT NULL,
///       processed_at TIMESTAMPTZ NOT NULL
///   );
///   ```
/// - [`MemoryState`]: nothing survives a restart, which starts over from
///   `start_after`.
///
/// A file is recorded when the input reads it, before the route wrote it
/// anywhere: on a crash, what was in flight is not read again.
#[async_trait::async_trait]
pub trait PollerState: FileInfoPollerState + Clone {
    /// Remember that `file` was read by the input `process_name`.
    async fn record(&self, process_name: &str, file: &FileInfo) -> ::file_store::Result;
}

#[async_trait::async_trait]
impl PollerState for sqlx::PgPool {
    async fn record(&self, process_name: &str, file: &FileInfo) -> ::file_store::Result {
        let mut tx = self.begin().await?;
        tx.record(process_name, file).await?;
        tx.commit().await?;
        Ok(())
    }
}

/// File key → file timestamp, by process name (the input's name).
type FilesRead = HashMap<String, HashMap<String, DateTime<Utc>>>;

/// [`PollerState`] in memory. Clones share it, so two runs of a route given
/// the same `MemoryState` resume where the first stopped.
#[derive(Debug, Clone, Default)]
pub struct MemoryState {
    files: Arc<Mutex<FilesRead>>,
}

impl MemoryState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the input `process_name` read the file `key`.
    pub fn contains(&self, process_name: &str, key: &str) -> bool {
        self.with(|files| {
            files
                .get(process_name)
                .is_some_and(|read| read.contains_key(key))
        })
    }

    fn with<T>(&self, f: impl FnOnce(&mut FilesRead) -> T) -> T {
        f(&mut self.files.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

#[async_trait::async_trait]
impl FileInfoPollerState for MemoryState {
    async fn latest_timestamp(
        &self,
        process_name: &str,
        _file_type: &str,
    ) -> ::file_store::Result<Option<DateTime<Utc>>> {
        Ok(self.with(|files| {
            files
                .get(process_name)
                .and_then(|read| read.values().max().copied())
        }))
    }

    async fn exists(&self, process_name: &str, file_info: &FileInfo) -> ::file_store::Result<bool> {
        Ok(self.with(|files| {
            files
                .get(process_name)
                .is_some_and(|read| read.contains_key(&file_info.key))
        }))
    }

    /// Forget files older than `offset`, the start of the poller's listing
    /// window, keeping the newest: it is the resume point.
    async fn clean(
        &self,
        process_name: &str,
        _file_type: &str,
        offset: DateTime<Utc>,
    ) -> ::file_store::Result<u64> {
        Ok(self.with(|files| {
            let Some(read) = files.get_mut(process_name) else {
                return 0;
            };
            let newest = read.values().max().copied();
            let before = read.len();
            read.retain(|_, ts| *ts >= offset || Some(*ts) == newest);
            (before - read.len()) as u64
        }))
    }
}

#[async_trait::async_trait]
impl PollerState for MemoryState {
    async fn record(&self, process_name: &str, file: &FileInfo) -> ::file_store::Result {
        self.with(|files| {
            files
                .entry(process_name.to_string())
                .or_default()
                .insert(file.key.clone(), file.timestamp);
        });
        Ok(())
    }
}

/// Hands file_store a [`PollerState`] as the recorder of the files read.
pub(super) struct Recorder<'a, S>(pub &'a S);

#[async_trait::async_trait]
impl<S: PollerState> FileInfoPollerStateRecorder for Recorder<'_, S> {
    async fn record(&mut self, process_name: &str, file_info: &FileInfo) -> ::file_store::Result {
        self.0.record(process_name, file_info).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(key: &str, minute: u32) -> FileInfo {
        FileInfo {
            key: key.to_string(),
            prefix: "p".to_string(),
            timestamp: DateTime::from_timestamp(i64::from(minute) * 60, 0).expect("in range"),
            size: 0,
        }
    }

    #[tokio::test]
    async fn memory_state_tracks_files_per_process() -> ::file_store::Result {
        let state = MemoryState::new();
        assert_eq!(state.latest_timestamp("a", "p").await?, None);

        state.record("a", &file("f1", 1)).await?;
        state.record("a", &file("f3", 3)).await?;
        state.record("b", &file("f9", 9)).await?;

        assert_eq!(
            state.latest_timestamp("a", "p").await?,
            Some(file("", 3).timestamp)
        );
        assert!(state.exists("a", &file("f1", 1)).await?);
        assert!(!state.exists("b", &file("f1", 1)).await?);

        // Everything is older than minute 10, but the newest stays.
        assert_eq!(state.clean("a", "p", file("", 10).timestamp).await?, 1);
        assert!(state.exists("a", &file("f3", 3)).await?);
        Ok(())
    }
}
