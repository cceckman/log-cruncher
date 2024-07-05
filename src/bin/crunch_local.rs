use log_cruncher::Cruncher;
use std::fs::File;
use std::path::Path;

fn main() {
    let src: &Path = Path::new("quarantine/example2.log.gz");
    let db: &Path = Path::new("quarantine/example.db");

    let src = File::open(src).expect("could not open example file");
    let cruncher = Cruncher::new(db).expect("could not open DB");

    cruncher
        .crunch_gz(std::io::BufReader::new(src))
        .expect("could not crunch records");
}
