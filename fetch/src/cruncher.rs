use crate::record::LogEntry;
use anyhow::{anyhow, Context};
use rusqlite::{named_params, Connection};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::task::JoinSet;

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
        for (i, entry) in data.iter().enumerate() {
            entry.store(&tx).with_context(|| format!("in entry {i}"))?;
        }
        tx.commit().context("could not commit transaction")?;
        Ok(())
    }

    /// Fill AS numbers in the database.
    pub async fn asn_catchup(&self) -> anyhow::Result<()> {
        let asns: Vec<u32> = {
            let conn = self.conn.lock().unwrap();
            let asns: Result<Vec<u32>, _> = conn
                .prepare("SELECT asn FROM autonomous_systems WHERE name IS NULL")
                .context("incorrect query for unnamed ASNs")?
                .query_map([], |row| row.get(0))
                .context("failed query for unnamed ASNs")?
                .collect();
            asns.context("failed for some unnamed ASNs")?
        };
        let client = Arc::new(reqwest::Client::new());
        let mut asn_queries = JoinSet::new();
        for asn in asns.into_iter() {
            let client = client.clone();
            asn_queries.spawn(async move { (asn, Self::peeringdb_asn_query(client, asn).await) });
        }
        let mut unknown_asns: Vec<u32> = Default::default();
        while let Some(res) = asn_queries.join_next().await {
            let conn = self.conn.lock().unwrap();
            let (asn, result) = res.unwrap();
            let name = match result {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!("could not get results for ASN {asn} from PeeringDB: {err}");
                    unknown_asns.push(asn);
                    continue;
                }
            };

            conn.prepare(
                r#"
                INSERT INTO autonomous_systems (asn, name) VALUES (:asn, :name)
                ON CONFLICT (asn) DO
                UPDATE SET name = :name WHERE asn = :asn;
                "#,
            )
            .map_err(|err| {
                anyhow!(
                    "invalid query to update ASN name: {} {}: {}",
                    asn,
                    &name,
                    err
                )
            })?
            .execute(named_params! { ":asn": asn, ":name": &name})
            .map_err(|err| {
                anyhow!(
                    "could not execute update to ASN name ({} {}): {}",
                    asn,
                    &name,
                    err
                )
            })?;
        }
        if unknown_asns.is_empty() {
            return Ok(());
        }

        // Compare all the remaining ones against Spamhaus.
        let drop_list = Self::spamhaus_droplist(&client)
            .await
            .map_err(|err| anyhow!("could not get DROP list from Spamhaus: {err}"))?;
        let conn = self.conn.lock().unwrap();
        for asn in unknown_asns.iter() {
            if let Some(name) = drop_list.get(asn) {
                let exec = conn
                    .prepare(
                        r#"
                INSERT INTO autonomous_systems (asn, name, droplist) VALUES (:asn, :name, :droplist)
                ON CONFLICT (asn) DO
                UPDATE SET name = :name , droplist = :droplist WHERE asn = :asn;
                "#,
                    )
                    .and_then(|mut c| {
                        c.execute(
                            named_params! {":asn": asn, ":name": name, ":droplist": "spamhaus"},
                        )
                    })
                    .map_err(|err| anyhow!("failed to insert of ASN entry with droplist: {err}"));
                if let Err(err) = exec {
                    tracing::error!("error: {err}");
                }
            }
        }

        Ok(())
    }

    /// Queries PeeringDB for the name of an ASN.
    async fn peeringdb_asn_query(client: Arc<reqwest::Client>, asn: u32) -> anyhow::Result<String> {
        // Response from PeeringDB's "list as-set by asn" API:
        // https://www.peeringdb.com/apidocs/
        #[derive(serde::Deserialize)]
        struct AsnResponse {
            data: Vec<HashMap<String, String>>,
            #[allow(dead_code)]
            meta: serde_json::Value,
        }

        let response = client
            .get(format!("https://www.peeringdb.com/api/as_set/{asn}"))
            .send()
            .await
            .with_context(|| format!("failed HTTP request for ASN {asn} info"))?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "failed HTTP request for ASN {asn} info: HTTP status {}",
                response.status()
            ));
        }
        let response_content: AsnResponse = response
            .json()
            .await
            .with_context(|| format!("failed to decode HTTP response for ASN {asn} info"))?;
        let this_asn_string = asn.to_string();
        response_content
            .data
            .into_iter()
            .flat_map(|m| m.into_iter())
            .filter(|(asn_string, _)| &this_asn_string == asn_string)
            .map(|(_, name)| name)
            .next()
            .ok_or_else(|| anyhow!("found no result from PeeringDB for ASN {asn}"))
    }

    /// Queries Spamhaus for the ASNs in the "don't route or peer" list.
    async fn spamhaus_droplist(client: &reqwest::Client) -> anyhow::Result<HashMap<u32, String>> {
        const DROPLIST_URL: &str = "https://www.spamhaus.org/drop/asndrop.json";

        // Response from PeeringDB's "list as-set by asn" API:
        // https://www.peeringdb.com/apidocs/
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum AsnResponse {
            Entry { asn: u32, asname: String },
            Metadata { copyright: String },
        }

        let response = client
            .get(DROPLIST_URL)
            .send()
            .await
            .context("failed HTTP request for Spamhaus droplist")?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "failed HTTP request for Spamhaus droplist: {}",
                response.status()
            ));
        }
        let response_bytes = response
            .bytes()
            .await
            .context("could not download body of Spamhaus droplist")?;
        serde_json::Deserializer::from_slice(response_bytes.as_ref())
            .into_iter::<AsnResponse>()
            // Manually "collect" into a result
            .try_fold(HashMap::new(), |mut asn_map, x| {
                let asn_resp = x?;
                match asn_resp {
                    AsnResponse::Entry { asn, asname } => {
                        asn_map.insert(asn, asname);
                    }
                    AsnResponse::Metadata { copyright } => {
                        // Spamhaus asks us to include the copyright; we don't put it in the DB,
                        // but we will output it here.
                        tracing::info!("Using data from Spamhaus: {copyright}");
                    }
                };
                Ok(asn_map)
            })
    }
}
