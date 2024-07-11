use std::{path::Path, sync::Mutex};

use crate::record::LogEntry;
use anyhow::Context;
use rusqlite::Connection;

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

    /// Add the entries to the database.
    pub fn crunch(&self, data: &[LogEntry]) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().context("could not begin transaction")?;
        for (i, entry) in data.into_iter().enumerate() {
            entry.store(&tx).with_context(|| format!("in entry {i}"))?;
        }
        tx.commit().context("could not commit transaction")?;
        Ok(())
    }
}
