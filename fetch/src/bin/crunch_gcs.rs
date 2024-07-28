use std::{
    cmp::{max, min},
    path::Path,
};

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

    // Keep the concurrency well under the FD limit,
    // so we don't run out of FDs for connections.
    let (soft_fd_limit, hard_fd_limit) =
        nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE)
            .expect("could not query FD limit");
    tracing::debug!("FD limit of {soft_fd_limit} (soft) / {hard_fd_limit} (hard)");
    // We artificially limit this, as I've been getting errors.
    let concurrency: usize = max(1, min(soft_fd_limit.saturating_sub(100), 128))
        .try_into()
        .expect("could not fit concurrency limit into usize");

    Cruncher {
        gcs_path,
        database: dbfile.to_owned(),
        // This seems to be the limiting factor when cleanup is enabled.
        // Tokio will handle the thread count for us;
        // this is just a memory limit. And we have a lot of memory.
        // We do have to keep it under the fd limit, though!
        concurrency,
        // Not convinced I'm not losing logs to this, so far.
        cleanup: true,
    }
    .crunch(&rt)
    .unwrap()
}
