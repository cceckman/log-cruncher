/* { "clientIP": "%{json.escape(req.http.fastly-client-ip)}V",
 "ispID": "%{json.escape(client.as.number)}V",
 "countryCode": "%{json.escape(client.geo.country_code)}V",
 "requests": "%{json.escape(client.requests)}V",
 "isIPv6": "%{json.escape(req.is_ipv6)}V",
 "isH2": "%{json.escape(fastly_info.is_h2)}V",
 "urlPath": "%{json.escape(req.url.path)}V",
 "httpReferer": "%{json.escape(req.http.referer)}V",
 "httpUA": "%{json.escape(req.http.user-agent)}V",
 "cacheState": "%{json.escape(fastly_info.state)}V",
 "respStatus": "%{json.escape(resp.status)}V",
 "respTotalBytes": "%{json.escape(resp.bytes_written)}V",
 "timeElapsed": "%{json.escape(time.elapsed.usec)}V",
 "reqStartTime": "%{json.escape(time.start)}V",
 }
*
*/

use std::{net::IpAddr, time::Duration};

use rusqlite::{named_params, Connection};
use serde::Deserialize;

const SCHEMA: &str = include_str!("schema.sql");

/// JSON log structure from Fastly.
///
/// This is specific to my log setup -- these are the fields I have configured.
#[derive(Debug, Deserialize)]
pub struct LogEntry {
    #[serde(rename = "clientIP")]
    client_ip: IpAddr,

    // ASNs were 2-byte until ~2007;
    // RFC 6793 formalized 4-byte ASN for BGP in 2021.
    #[serde(rename = "ispID")]
    asn: u32,

    #[serde(rename = "countryCode")]
    country_code: Option<String>,

    requests: usize,
    #[serde(rename = "isIPv6")]
    ipv6: bool,
    #[serde(rename = "isH2")]
    http2: bool,
    url_path: String,
    #[serde(rename = "httpReferer")]
    referer: Option<String>,
    #[serde(rename = "httpUA")]
    user_agent: Option<String>,
    #[serde(rename = "cacheState")]
    cache_state: String,
    #[serde(rename = "respStatus")]
    status: usize,
    #[serde(rename = "respTotalBytes")]
    response_bytes: usize,
    #[serde(rename = "timeElapsed")]
    response_duration: Duration,
    #[serde(rename = "reqStartTime")]
    request_start_time: String,
}

fn get_ipv4(ip: &IpAddr) -> Option<String> {
    match ip {
        IpAddr::V4(v) => Some(v.to_string()),
        _ => None,
    }
}

fn get_ipv6(ip: &IpAddr) -> Option<String> {
    match ip {
        IpAddr::V6(v) => Some(v.to_string()),
        _ => None,
    }
}

impl LogEntry {
    fn store(&self, conn: &mut Connection) -> Result<(), rusqlite::Error> {
        let mut tx = conn.transaction()?;
        let _ = tx
            .prepare_cached(
                "INSERT INTO client_ips (ipv4, ipv6) VALUES (?, ?) ON CONFLICT IGNORE;",
            )?
            .execute([get_ipv4(&self.client_ip), get_ipv6(&self.client_ip)])?;
        let ip_id = tx
            .prepare_cached("SELECT FROM client_ips (id) WHERE ipv4 = ? AND ipv6 = ?);")?
            .execute([get_ipv4(&self.client_ip), get_ipv6(&self.client_ip)])?;
        tx.prepare_cached("INSERT INTO paths (path) VALUES (?) ON CONFLICT IGNORE;")?
            .execute([&self.url_path])?;
        if let Some(referer) = &self.referer {
            tx.prepare_cached("INSERT INTO referers (referer) VALUES (?) ON CONFLICT IGNORE;")?
                .execute([&referer])?;
        }
        if let Some(user_agent) = &self.user_agent {
            tx.prepare_cached(
                "INSERT INTO user_agents (user_agent) VALUES (?) ON CONFLICT IGNORE;",
            )?
            .execute([&user_agent])?;
        }
        tx.prepare_cached(
            r#"
INSERT INTO requests (
    client_ip, asn, country_code,
    requests, ipv6, http2, cache_state,
    response_bytes, response_duration,
    request_start_time, url_path, referer, user_agent
) VALUES (
   :client_ip,:asn,:country_code,
   :requests,:ipv6,:http2,:cache_state,
   :response_bytes,:response_duration,
   :request_start_time,
   ( SELECT id FROM paths WHERE path = :url_path),
   ( SELECT id FROM referers WHERE referer = :referer),
   ( SELECT id FROM user_agents WHERE user_agent = :user_agent),
)"#,
        )?
        .execute(named_params! {
            ":client_ip": ip_id,
            ":asn": self.asn as usize,
            ":country_code": &self.country_code,
            ":requests": self.requests,
            ":ipv6": self.ipv6,
            ":http2": self.http2,
            ":cache_state": &self.cache_state,
            ":response_bytes": self.response_bytes,
            ":response_duration": self.response_duration.as_secs_f32(),
            ":request_start_time": &self.request_start_time,
            ":url_path": &self.url_path,
            ":user_agent": &self.user_agent,
            ":referer": &self.referer,
        })?;
        tx.commit()
    }
}
