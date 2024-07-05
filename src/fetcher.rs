//! Fetcher for log entries from backing storage.
//!

use anyhow::Context;
use opendal::{
    layers::{BlockingLayer, LoggingLayer, TracingLayer},
    Operator,
};

/// LogSet is a handle to a set of logs.
pub struct LogSet {
    name: String,
    data: Vec<u8>,
    source: Arc<Fetcher>,
}

impl LogSet<'_> {
    /// Mark this set of logs as processed, successfully or unsuccessfully.
    ///
    /// Returns an error that includes the original result.
    pub async fn complete(status: anyhow::Result<()>) -> anyhow::Result<()> {
        todo!()
    }
}

impl Drop for LogSet<'_> {
    fn drop(&mut self) {
        todo!()
    }
}

/// Fetches log chunks from a backing store.
pub struct Fetcher {
    operator: opendal::Operator,
}

impl Fetcher {
    pub fn new_gcs(bucket: &str) -> anyhow::Result<Self> {
        let mut builder = opendal::services::Gcs::default();
        builder.bucket(bucket);
        let operator = Operator::new(builder)?.layer(TracingLayer).finish();
        Ok(Fetcher { operator })
    }

    /// Start the fetch process.
    /// Buffer at most N log chunks at a time.
    pub async fn fetch(self: &Arc<Self>, buffer: usize) -> tokio::sync::mpsc::Receiver<anyhow::Result<LogSet>> {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        rx
    }

    fn delete_object(&self, object: &name) {

    }
}
