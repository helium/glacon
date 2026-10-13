use super::{
    FileMeta, GlaconInput, InputSettings,
    state::{PollerState, Recorder},
};
use crate::metrics;
use ::file_store::{file_info_poller::FileInfoStream, file_source};
use futures::StreamExt;
use glacon_core::{
    Batch, BoxError, GlaconCoreInput, GlaconCoreInputInit, InitializedInput, InputContext,
};
use tokio::sync::mpsc;

/// A [`GlaconCoreInput`] polling one S3 bucket with `file_store`: each file
/// is one batch, its protobuf messages decoded by a [`GlaconInput`]. What it
/// read is kept in a [`PollerState`]. The poller is its background task,
/// supervised by the route.
///
/// ```ignore
/// route.input("us", FileStoreInput::new(SpeedTestInput, us_settings, pool.clone()));
/// ```
pub struct FileStoreInput<I, S> {
    input: I,
    settings: InputSettings,
    state: S,
}

impl<I: GlaconInput, S: PollerState> FileStoreInput<I, S> {
    pub fn new(input: I, settings: InputSettings, state: S) -> Self {
        Self {
            input,
            settings,
            state,
        }
    }
}

#[async_trait::async_trait]
impl<I, S> GlaconCoreInputInit<I::Record> for FileStoreInput<I, S>
where
    I: GlaconInput,
    S: PollerState,
{
    /// Connect to the bucket and build its poller: the poller is returned for
    /// the route to run, and the running input receives the files it finds.
    async fn init(
        self: Box<Self>,
        ctx: &InputContext,
    ) -> Result<InitializedInput<I::Record>, BoxError> {
        let FileStoreInput {
            input,
            settings,
            state,
        } = *self;
        let bucket = settings.bucket.connect().await;
        let builder = file_source::Continuous::prost_source::<I::Proto, _, _>()
            .state(state.clone())
            .bucket_client(bucket)
            .prefix(input.prefix())
            .process_name(ctx.input().to_string())
            .lookback_start_after(settings.start_after)
            .poll_duration_opt(settings.poll_duration)
            .idle_timeout_opt(settings.idle_timeout);
        let builder = match settings.offset {
            Some(offset) => builder.offset(offset),
            None => builder,
        };
        let (files, poller) = builder.create().await?;

        let running = RunningFileStoreInput {
            input,
            state,
            ctx: ctx.clone(),
            files,
        };
        Ok(InitializedInput::new(running).with_task(poller))
    }
}

/// A [`FileStoreInput`] once initialized: decodes the files its poller sends.
struct RunningFileStoreInput<I: GlaconInput, S> {
    input: I,
    state: S,
    ctx: InputContext,
    files: mpsc::Receiver<FileInfoStream<I::Proto>>,
}

#[async_trait::async_trait]
impl<I, S> GlaconCoreInput<I::Record> for RunningFileStoreInput<I, S>
where
    I: GlaconInput,
    S: PollerState,
{
    /// The next file, decoded. `None` once the poller stopped (its idle
    /// timeout, or an error, which the route reports from the poller's
    /// task).
    async fn next_batch(&mut self) -> Result<Option<Batch<I::Record>>, BoxError> {
        match self.files.recv().await {
            Some(file) => Ok(Some(
                decode(&self.input, &self.state, &self.ctx, file).await?,
            )),
            None => Ok(None),
        }
    }
}

/// Decode every message of a polled file, logging and counting failures.
/// Records the file as read in `state`.
async fn decode<I: GlaconInput, S: PollerState>(
    input: &I,
    state: &S,
    ctx: &InputContext,
    file: FileInfoStream<I::Proto>,
) -> Result<Batch<I::Record>, BoxError> {
    let info = file.file_info.clone();
    let protos: Vec<I::Proto> = file
        .into_stream(&mut Recorder(state))
        .await?
        .collect()
        .await;

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
