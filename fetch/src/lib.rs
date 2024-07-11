mod cruncher;
mod fetcher;
mod record;
mod streamhack;

use anyhow::Context;
use record::LogEntry;
use std::{
    io::{self},
    path::{Path, PathBuf},
    sync::Arc,
};
use streamhack::CommaHacker;
use tokio::runtime::Runtime;

use fetcher::Fetcher;

/// LogSet is a handle to a set of logs.
pub struct LogSet<T> {
    pub name: String,
    pub data: Vec<T>,
    source: Arc<Fetcher>,
}

impl TryFrom<LogSet<u8>> for LogSet<LogEntry> {
    type Error = anyhow::Error;

    fn try_from(value: LogSet<u8>) -> Result<Self, Self::Error> {
        // Decompress the record.
        let cursor = io::Cursor::new(value.data);
        let cursor = flate2::bufread::GzDecoder::new(cursor);
        // ...and get rid of trailing commas at top-level JSON objects. Oops.
        let cursor = CommaHacker::new(std::io::BufReader::new(cursor));
        let entries: anyhow::Result<Vec<LogEntry>> = serde_json::Deserializer::from_reader(cursor)
            .into_iter()
            .enumerate()
            .map(|(i, result)| result.with_context(|| format!("JSON parse error in entry {i}")))
            .collect();
        Ok(LogSet {
            data: entries.with_context(|| format!("in log set {}", &value.name))?,
            name: value.name,
            source: value.source,
        })
    }
}

/// Fetch and crunch the logs into the database.
pub struct Cruncher {
    pub gcs_path: String,
    pub database: PathBuf,
    pub concurrency: usize,

    /// Delete the logs after completion
    pub cleanup: bool,
}

impl Cruncher {
    /// Fetch and crunch the logs.
    pub fn crunch(self, rt: &Runtime) -> anyhow::Result<()> {
        let fetcher = Fetcher::new_gcs(&self.gcs_path, self.cleanup)
            .context("could not initialize fetcher")?;
        let fetcher = Arc::new(fetcher);

        let mut log_sets = rt.block_on(async { fetcher.fetch(self.concurrency).await });

        rt.block_on(async move {
            let mut ok = 0;
            let mut err = 0;
            let cruncher = cruncher::Cruncher::new(&self.database)?;
            while let Some(log_set) = log_sets.recv().await {
                let log_set = log_set.context("got error in streaming log sets")?;
                tracing::info!("processing log set {}", &log_set.name);
                let crunch_result = cruncher
                    .crunch(&log_set.data)
                    .with_context(|| format!("error in processing log file {}", log_set.name));
                tracing::info!(
                    "completed log set {}, result: {}",
                    &log_set.name,
                    if crunch_result.is_ok() { "ok" } else { "error" }
                );
                if crunch_result.is_ok() {
                    ok += 1
                } else {
                    err += 1
                };
                let name = log_set.name.clone();
                if let Err(e) = log_set.complete(crunch_result).await {
                    tracing::error!("error finalizing log set {}: {}", &name, e);
                }
            }
            tracing::info!("crunched {} logsets: {} ok, {} errors", ok + err, ok, err);
            if let Err(err) = cruncher.asn_catchup().await {
                tracing::error!("errors in updating ASN table: {}", err);
            } else {
                tracing::info!("ASN table up to date");
            }
            Ok(())
        })
    }
}
