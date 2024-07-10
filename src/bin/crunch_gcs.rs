use std::path::Path;

use log_cruncher::Cruncher;

fn main() {
    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    Cruncher {
        gcs_path: "fastly-logs.cceckman.com".to_string(),
        database: Path::new("quarantine/gcs.db").to_owned(),
        concurrency: 10,
        cleanup: false,
    }
    .crunch(&rt)
    .unwrap()
}
