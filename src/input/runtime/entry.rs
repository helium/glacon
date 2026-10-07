use super::{
    DecodedFile, DecodedFiles, InputSpec, noop_recorder::NoopRecorder, poller_state::PollerState,
};
use crate::{
    FileMeta, GlaconInput, InputSettings, Result, metrics, state::StateDb, tasks::NamedTask,
};
use file_store::{file_info_poller::FileInfoStream, file_source};
use futures::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio_stream::wrappers::ReceiverStream;

/// An input as added to a route: its name, implementation and settings.
pub(crate) struct InputEntry<I> {
    pub name: String,
    pub input: I,
    pub settings: InputSettings,
}

#[async_trait::async_trait]
impl<I> InputSpec<I::Record> for InputEntry<I>
where
    I: GlaconInput,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(
        self: Box<Self>,
        route: &str,
        db: Arc<StateDb>,
        files_retention: Duration,
    ) -> Result<(DecodedFiles<I::Record>, NamedTask)> {
        let InputEntry {
            name,
            input,
            settings,
        } = *self;

        let bucket = settings.bucket.connect().await;
        let builder = file_source::Continuous::prost_source::<I::Proto, _, _>()
            .state(PollerState {
                route: route.to_string(),
                db,
                retention: files_retention,
            })
            .bucket_client(bucket)
            .prefix(input.prefix())
            .process_name(name.clone())
            .lookback_start_after(settings.start_after)
            .poll_duration_opt(settings.poll_duration)
            .idle_timeout_opt(settings.idle_timeout);
        let builder = match settings.offset {
            Some(offset) => builder.offset(offset),
            None => builder,
        };
        let (receiver, poller) = builder.create().await?;

        let input = Arc::new(input);
        let input_name: Arc<str> = Arc::from(name.as_str());
        let route: Arc<str> = Arc::from(route);
        let files = ReceiverStream::new(receiver)
            .then(move |file| decode(input.clone(), input_name.clone(), route.clone(), file))
            .boxed();

        Ok((files, NamedTask::new(format!("poller {name}"), poller)))
    }
}

/// Decode every message of a polled file, logging and counting failures.
async fn decode<I: GlaconInput>(
    input: Arc<I>,
    input_name: Arc<str>,
    route: Arc<str>,
    file: FileInfoStream<I::Proto>,
) -> Result<DecodedFile<I::Record>> {
    let info = file.file_info.clone();
    let protos: Vec<I::Proto> = file.into_stream(&mut NoopRecorder).await?.collect().await;

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
        &route,
        &input_name,
        records.len() as u64,
    );

    if failures > 0 {
        tracing::error!(
            route = %route,
            input = %input_name,
            file = %info.key,
            failures,
            total,
            "failed to decode messages"
        );
        metrics::input_counter(metrics::DECODE_FAILURES, &route, &input_name, failures);
    }

    Ok(DecodedFile {
        input: input_name,
        file: info,
        records,
    })
}
