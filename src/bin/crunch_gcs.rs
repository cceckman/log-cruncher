use anyhow::Context;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use log_cruncher::Cruncher;
use log_cruncher::Fetcher;

fn main() {
    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let fetcher = Fetcher::new_gcs("fastly-logs.cceckman.com", /*cleanup=*/ false).unwrap();
    let fetcher = Arc::new(fetcher);

    let mut log_sets = rt.block_on(async { fetcher.fetch(10).await });
    // TODO: May be better to do the decoding in multiple threads--
    // the JSON parsing can proceed in parallel even if commit-to-the-DB needs to serialize.
    let result: anyhow::Result<()> = rt.block_on(async move {
        let cruncher = Cruncher::new(Path::new("quarantine/gcs.db"))?;
        while let Some(log_set) = log_sets.recv().await {
            let log_set = log_set.context("got error in streaming log sets")?;
            tracing::info!("processing log set {}", &log_set.name);
            let data = Cursor::new(&log_set.data);
            let crunch_result = cruncher
                .crunch_gz(data)
                .with_context(|| format!("error in processing log file {}", log_set.name));
            tracing::info!(
                "completed log set {}, result: {}",
                &log_set.name,
                if crunch_result.is_ok() { "ok" } else { "error" }
            );
            log_set.complete(crunch_result).await?;
        }
        Ok(())
    });
    result.expect("failed processing logs");
}
