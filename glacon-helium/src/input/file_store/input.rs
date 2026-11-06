use super::{
    FileMeta, FileStoreInputSettings, GlaconInput,
    state::{LedgerState, NoRecord},
};
use crate::metrics;
use ::file_store::{file_info_poller::FileInfoStream, file_source};
use futures::StreamExt;
use glacon_core::{Batch, BoxError, GlaconCoreInput, InitializedInput, InputContext};
use tokio::sync::mpsc;

/// A [`GlaconCoreInput`] polling one S3 bucket with `file_store`: each file
/// is one batch, keyed by its S3 key, its protobuf messages decoded by a
/// [`GlaconInput`]. The poller is its background task, supervised by the
/// route.
///
/// The poller's state is the route's ledger: it resumes at the oldest file
/// not delivered everywhere, and skips the files that were. Files stay in
/// their bucket, so a file is read again after a crash until it is done.
///
/// ```ignore
/// route.input("us", FileStoreInput::new(SpeedTestInput, us_settings));
/// ```
pub struct FileStoreInput<I> {
    input: I,
    settings: FileStoreInputSettings,
}

impl<I: GlaconInput> FileStoreInput<I> {
    pub fn new(input: I, settings: FileStoreInputSettings) -> Self {
        Self { input, settings }
    }
}

#[async_trait::async_trait]
impl<I: GlaconInput> GlaconCoreInput<I::Record> for FileStoreInput<I> {
    type State = FileStoreState<I>;

    /// Connect to the bucket and build its poller: the poller is returned for
    /// the route to run, and the state receives the files it finds.
    async fn init(
        &mut self,
        ctx: &InputContext,
    ) -> Result<InitializedInput<Self::State>, BoxError> {
        let settings = &self.settings;
        let state = LedgerState {
            ledger: ctx.ledger().clone(),
        };
        let bucket = settings.bucket.connect().await;
        let builder = file_source::Continuous::prost_source::<I::Proto, _, _>()
            .state(state)
            .bucket_client(bucket)
            .prefix(self.input.prefix())
            .process_name(ctx.input().to_string())
            .lookback_start_after(settings.start_after)
            .poll_duration_opt(settings.poll_duration)
            .idle_timeout_opt(settings.idle_timeout);
        let builder = match settings.offset {
            Some(offset) => builder.offset(offset),
            None => builder,
        };
        let (files, poller) = builder.create().await?;

        let state = FileStoreState {
            ctx: ctx.clone(),
            files,
        };
        Ok(InitializedInput::new(state).with_task(poller))
    }

    /// The next file, decoded. `None` once the poller stopped (its idle
    /// timeout, or an error, which the route reports from the poller's
    /// task).
    async fn next_batch(
        &mut self,
        state: &mut Self::State,
    ) -> Result<Option<Batch<I::Record>>, BoxError> {
        match state.files.recv().await {
            Some(file) => Ok(Some(decode(&self.input, &state.ctx, file).await?)),
            None => Ok(None),
        }
    }
}

/// The state of a [`FileStoreInput`]: the files its poller sends.
pub struct FileStoreState<I: GlaconInput> {
    ctx: InputContext,
    files: mpsc::Receiver<FileInfoStream<I::Proto>>,
}

/// Decode every message of a polled file, logging and counting failures.
/// The route records it in its ledger.
async fn decode<I: GlaconInput>(
    input: &I,
    ctx: &InputContext,
    file: FileInfoStream<I::Proto>,
) -> Result<Batch<I::Record>, BoxError> {
    let info = file.file_info.clone();
    let protos: Vec<I::Proto> = file.into_stream(&mut NoRecord).await?.collect().await;

    let total = protos.len();
    let records: Vec<I::Record> = protos
        .into_iter()
        .enumerate()
        .filter_map(|(index, proto)| {
            let meta = FileMeta {
                key: &info.key,
                timestamp: info.timestamp,
                index,
            };
            input.decode(proto, meta)
        })
        .collect();
    let failures = (total - records.len()) as u64;

    metrics::input_counter(
        metrics::RECORDS_DECODED,
        ctx.route(),
        ctx.input(),
        records.len() as u64,
    );
    if failures > 0 {
        tracing::error!(
            route = %ctx.route(),
            input = %ctx.input(),
            file = %info.key,
            failures,
            total,
            "failed to decode messages"
        );
        metrics::input_counter(metrics::DECODE_FAILURES, ctx.route(), ctx.input(), failures);
    }

    Ok(Batch {
        key: info.key,
        timestamp: info.timestamp,
        records,
    })
}
