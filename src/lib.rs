use std::{io::BufRead, path::Path, sync::Mutex};

use anyhow::Context;
use record::LogEntry;
use rusqlite::Connection;
use streamhack::CommaHacker;

mod record;
mod streamhack;

/// Consumer of logs.
pub struct Cruncher {
    conn: Mutex<Connection>,
}

const SCHEMA: &str = include_str!("schema.sql");

impl Cruncher {
    /// Create a new Cruncher, which collates log records into a database.
    pub fn new(db: &Path) -> anyhow::Result<Self> {
        let mut conn = Connection::open(db).context("could not open DB")?;
        {
            let tx = conn.transaction().context("could not initialize DB")?;
            tx.execute_batch(SCHEMA)
                .context("could not initialize DB schema")?;
            tx.commit()?;
        }

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Add the records from the provided GZipped buffer to the result.
    pub fn crunch_gz(&self, data: impl BufRead) -> anyhow::Result<()> {
        // Decompress the record.
        let cursor = flate2::bufread::GzDecoder::new(data);
        // ...and get rid of trailing commas at top-level JSON objects. Oops.
        let cursor = CommaHacker::new(std::io::BufReader::new(cursor));
        let entries: anyhow::Result<Vec<LogEntry>> = serde_json::Deserializer::from_reader(cursor)
            .into_iter()
            .enumerate()
            .map(|(i, result)| result.with_context(|| format!("JSON parse error in entry {i}")))
            .collect();
        let entries = entries?;

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().context("could not begin transaction")?;
        for (i, entry) in entries.into_iter().enumerate() {
            entry.store(&tx).with_context(|| format!("in entry {i}"))?;
        }
        tx.commit().context("could not commit transaction")?;
        Ok(())
    }
}
