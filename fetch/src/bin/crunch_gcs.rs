use std::path::Path;

use log_cruncher::Cruncher;

/// Usage: (bucket) (dbfile)
fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 3, "requires arguments (gcs_path) and (dbfile)");
    let gcs_path = args[1].clone();
    let dbfile = Path::new(&args[2]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    Cruncher {
        gcs_path,
        database: dbfile.to_owned(),
        // This seems to be the limiting factor when cleanup is enabled.
        // Tokio will handle the thread count for us;
        // this is just a memory limit. And we have a lot of memory.
        concurrency: 1024,
        // Not convinced I'm not losing logs to this, so far.
        cleanup: true,
    }
    .crunch(&rt)
    .unwrap()
}
