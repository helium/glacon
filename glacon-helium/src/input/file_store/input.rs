use super::{
    FileMeta, GlaconInput, InputSettings,
    state::{PollerState, Recorder},
};
use crate::metrics;
use ::file_store::{file_info_poller::FileInfoStream, file_source};
use futures::StreamExt;
use glacon_core::{Batch, BoxError, GlaconCoreInput, InputContext};
use tokio::{sync::mpsc, task::JoinHandle};

/// A [`GlaconCoreInput`] polling one S3 bucket with `file_store`: each file
/// is one batch, its protobuf messages decoded by a [`GlaconInput`]. What it
/// read is kept in a [`PollerState`].
///
/// ```ignore
/// route.input("us", FileStoreInput::new(SpeedTestInput, us_settings, pool.clone()));
/// ```
pub struct FileStoreInput<I: GlaconInput, S> {
    input: I,
    settings: InputSettings,
    state: S,
    /// Set by `init`.
    polling: Option<Polling<I::Proto>>,
}

/// The running poller.
struct Polling<P> {
    ctx: InputContext,
    files: mpsc::Receiver<FileInfoStream<P>>,
    stop: triggered::Trigger,
    /// `None` once joined.
    poller: Option<JoinHandle<::file_store::Result>>,
}

impl<I: GlaconInput, S: PollerState> FileStoreInput<I, S> {
    pub fn new(input: I, settings: InputSettings, state: S) -> Self {
        Self {
            input,
            settings,
            state,
            polling: None,
        }
    }
}

#[async_trait::async_trait]
impl<I, S> GlaconCoreInput<I::Record> for FileStoreInput<I, S>
where
    I: GlaconInput,
    S: PollerState,
{
    /// Connect to the bucket and start polling it.
    async fn init(&mut self, ctx: &InputContext) -> Result<(), BoxError> {
        let settings = &self.settings;
        let bucket = settings.bucket.connect().await;
        let builder = file_source::Continuous::prost_source::<I::Proto, _, _>()
            .state(self.state.clone())
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
        let (stop, stopped) = triggered::trigger();

        self.polling = Some(Polling {
            ctx: ctx.clone(),
            files,
            stop,
            poller: Some(tokio::spawn(poller.run(stopped))),
        });
        Ok(())
    }

    /// The next file, decoded. `None` once the poller stopped on its own
    /// (idle timeout); its error if it failed.
    async fn next_batch(&mut self) -> Result<Option<Batch<I::Record>>, BoxError> {
        let polling = self.polling.as_mut().ok_or("not initialized")?;
        match polling.files.recv().await {
            Some(file) => Ok(Some(
                decode(&self.input, &self.state, &polling.ctx, file).await?,
            )),
            None => {
                polling.join().await?;
                Ok(None)
            }
        }
    }

    /// Stop the poller and wait for it.
    async fn terminate(&mut self) -> Result<(), BoxError> {
        match self.polling.take() {
            Some(mut polling) => {
                polling.stop.trigger();
                polling.join().await
            }
            None => Ok(()),
        }
    }
}

impl<P> Polling<P> {
    async fn join(&mut self) -> Result<(), BoxError> {
        match self.poller.take() {
            Some(poller) => Ok(poller.await??),
            None => Ok(()),
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
