//! The single task of a route that turns decoded files into output rows.
//!
//! Files from every input are handled one at a time, so dedup, the delivery
//! ledger and the processed-file record never race.

use crate::{
    Destination, GlaconMiddleware, GlaconRecord, OutputId, Result,
    input::{DecodedFile, DecodedFiles},
    metrics,
    output::OutputSink,
    state::{KeyHash, StateDb, hash_key},
};
use chrono::Utc;
use futures::StreamExt;
use std::{collections::HashSet, sync::Arc, time::Instant};

pub(crate) struct Processor<R> {
    pub route: Arc<str>,
    pub db: Arc<StateDb>,
    pub middleware: Box<dyn GlaconMiddleware<R>>,
    /// Indexed by `OutputId`.
    pub outputs: Vec<Box<dyn OutputSink<R>>>,
}

impl<R: GlaconRecord> Processor<R> {
    pub async fn run(
        mut self,
        mut files: DecodedFiles<R>,
        shutdown: triggered::Listener,
    ) -> Result {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!(route = %self.route, "shutdown requested, stopping processor");
                    break;
                }
                file = files.next() => match file {
                    Some(file) => self.process(file?).await?,
                    None => {
                        tracing::info!(route = %self.route, "all inputs finished, stopping processor");
                        break;
                    }
                }
            }
        }
        // Dropping `self` drops the writer handles, letting each writer task
        // flush its spool and exit.
        Ok(())
    }

    async fn process(&mut self, file: DecodedFile<R>) -> Result {
        let started = Instant::now();
        let DecodedFile {
            input,
            file,
            mut records,
        } = file;
        let route = &*self.route;

        let (key_hashes, dedup_dropped) = self.dedup(&mut records).await?;
        metrics::input_counter(metrics::DEDUP_DROPPED, route, &input, dedup_dropped);

        let mut middleware_dropped = 0u64;
        for record in &records {
            match self.middleware.route(record) {
                Destination::All => self.outputs.iter_mut().for_each(|o| o.stage(record)),
                Destination::Only(set) if !set.is_empty() => {
                    for (i, output) in self.outputs.iter_mut().enumerate() {
                        if set.contains(OutputId(i as u8)) {
                            output.stage(record);
                        }
                    }
                }
                Destination::Only(_) | Destination::Drop => middleware_dropped += 1,
            }
        }
        metrics::route_counter(metrics::MIDDLEWARE_DROPPED, route, middleware_dropped);

        // Outputs that took this file before an interrupted attempt keep what
        // they already have.
        let delivered = self.db.in_flight_deliveries(&input, &file.key).await?;
        for output in &mut self.outputs {
            if delivered.contains(output.name()) {
                output.discard();
                continue;
            }
            let (routed, rows) = output.flush().await?;
            if rows > 0 {
                self.db
                    .record_in_flight_delivery(&input, &file.key, output.name())
                    .await?;
            }
            metrics::output_counter(metrics::OUTPUT_RECORDS, route, output.name(), routed);
            metrics::output_counter(metrics::OUTPUT_ROWS, route, output.name(), rows);
        }

        let now = Utc::now();
        self.db
            .complete_file(&input, &file.key, file.timestamp, &key_hashes, now)
            .await?;
        let pruned = self.db.prune_dedup_keys_if_due(now).await?;
        metrics::state_pruned(route, "dedup_keys", pruned);

        let lag = (now - file.timestamp).to_std().unwrap_or_default();
        metrics::file_processed(route, &input, lag, started.elapsed());
        tracing::info!(
            route,
            input = %input,
            file = %file.key,
            records = records.len(),
            dedup_dropped,
            middleware_dropped,
            "processed file"
        );
        Ok(())
    }

    /// Drop records whose key repeats within the file or was already seen.
    /// Returns the hashes of every key in the file (to remember) and the
    /// number of records dropped.
    async fn dedup(&self, records: &mut Vec<R>) -> Result<(Vec<KeyHash>, u64)> {
        let before = records.len();
        let mut in_file = HashSet::new();
        records.retain(|r| {
            r.dedup_key()
                .is_none_or(|key| in_file.insert(hash_key(key)))
        });

        let key_hashes: Vec<KeyHash> = in_file.into_iter().collect();
        let known = self.db.known_keys(&key_hashes).await?;
        if !known.is_empty() {
            records.retain(|r| {
                r.dedup_key()
                    .is_none_or(|key| !known.contains(&hash_key(key)))
            });
        }

        Ok((key_hashes, (before - records.len()) as u64))
    }
}
