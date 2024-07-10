//! Fetcher for log entries from backing storage.
//!

use crate::LogSet;
use std::sync::Arc;

use anyhow::Context;
use opendal::{layers::TracingLayer, Operator};
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;

/// Fetches log chunks from a backing store.
pub struct Fetcher {
    operator: opendal::Operator,
    cleanup: bool,
}

impl<T> LogSet<T> {
    /// Mark this set of logs as processed, successfully or unsuccessfully.
    ///
    /// Returns the original error and/or an error in cleanup.
    pub async fn complete(self, status: anyhow::Result<()>) -> anyhow::Result<()> {
        if status.is_ok() {
            // Clean up the object from storage.
            return self
                .source
                .delete_object(&self.name)
                .await
                .context("failed to delete object: ");
        }
        // Don't clean it up.
        status.with_context(|| format!("in handling object {}: ", &self.name))
    }
}

impl Fetcher {
    /// Create a new fetcher from GCS buckets.
    ///
    /// Cleanup indicates whether successfully logged objects should be deleted from storage.
    pub fn new_gcs(bucket: &str, cleanup: bool) -> anyhow::Result<Self> {
        let mut builder = opendal::services::Gcs::default();
        builder.bucket(bucket);
        let operator = Operator::new(builder)?.layer(TracingLayer).finish();
        Ok(Fetcher { operator, cleanup })
    }

    /// Start the fetch process, returning a stream of logs.
    /// Buffer at most N log chunks at a time.
    pub async fn fetch(
        self: &Arc<Self>,
        buffer: usize,
    ) -> tokio::sync::mpsc::Receiver<anyhow::Result<LogSet<u8>>> {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        tokio::spawn({
            let fetcher = Arc::clone(self);
            let tx_ch = tx.clone();
            async move {
                if let Err(e) = fetcher.fetch_loop(tx_ch).await {
                    // Ignore a send error; likely hung up
                    let _ = tx.send(Err(e)).await;
                }
            }
        });
        rx
    }

    async fn fetch_loop(
        self: Arc<Self>,
        tx: Sender<anyhow::Result<LogSet<u8>>>,
    ) -> anyhow::Result<()> {
        let mut lister = self
            .operator
            .lister("")
            .await
            .context("could not list entries from storage")?;
        while let Some(entry) = lister.next().await {
            match entry.context("in listing bucket entries: ") {
                Err(e) => tx
                    .send(Err(e))
                    .await
                    .context("could not propagate error from fetch loop: ")?,
                Ok(v) => {
                    // We spawn an executor for every source,
                    // but we only start the fetch once we have a permit from
                    // the Sender. We might have a lot of Futures, but only a few active.
                    //
                    // What I'd _like_ to do is have the permit claimed in the spawner,
                    // and passed in to the worker task -- so the concurrency limits the number
                    // of tasks as well. But the permit closes over the lifetime of the Sender,
                    // which requires some sort of async spawn_scoped.
                    // There's some efforts to that end --
                    // from https://without.boats/blog/the-scoped-task-trilemma/,
                    // https://docs.rs/async_nursery/latest/async_nursery/
                    // looks viable?
                    // -- but I'm not going to try it yet.
                    // TODO: Try out async_nursery?
                    let tx = tx.clone();
                    let fetcher = Arc::clone(&self);
                    tokio::spawn(async move {
                        if let Ok(permit) = tx
                            .reserve()
                            .await
                            .context("could not prepare to send from fetch loop: ")
                        {
                            permit.send(fetcher.fetch_one(v.path()).await);
                        }
                    });
                }
            }
        }
        Ok(())
    }

    async fn fetch_one(self: Arc<Self>, path: &str) -> anyhow::Result<LogSet<u8>> {
        let rd = self
            .operator
            .reader(path)
            .await
            .with_context(|| format!("failed to start read of object {}: ", path))?;
        let data = rd
            .read(0..)
            .await
            .with_context(|| format!("failed to read object contents {}: ", path))?;
        Ok(LogSet {
            name: path.to_string(),
            data: data.to_vec(),
            source: self,
        })
    }

    async fn delete_object(&self, object: &str) -> anyhow::Result<()> {
        if self.cleanup {
            self.operator
                .delete(object)
                .await
                .with_context(|| format!("could not delete object {}: ", object))
        } else {
            Ok(())
        }
    }
}
