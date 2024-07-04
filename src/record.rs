//! Decode log entries from Fastly's JSON to SQL.
//!
//! A useful tool for generating custom log formats with the given fields:
//!
//! https://www.fastly.com/documentation/guides/integrations/logging/#custom-log-formatter

use std::{fmt::Display, net::IpAddr, str::FromStr, time::Duration};

use chrono::{DateTime, FixedOffset, Utc};
use rusqlite::{named_params, Transaction};
use serde::{Deserialize, Deserializer};

/// JSON log structure from Fastly.
///
/// This is specific to my log setup -- these are the fields I have configured.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct LogEntry {
    #[serde(rename = "clientIP")]
    client_ip: IpAddr,

    // ASNs were 2-byte until ~2007;
    // RFC 6793 formalized 4-byte ASN for BGP in 2021.
    #[serde(rename = "ispID", deserialize_with = "deserialize_number_from_string")]
    asn: u32,

    #[serde(rename = "countryCode")]
    country_code: Option<String>,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    requests: usize,
    #[serde(
        rename = "isIPv6",
        deserialize_with = "deserialize_bool_from_bitstring"
    )]
    ipv6: bool,
    #[serde(rename = "isH2", deserialize_with = "deserialize_bool_from_bitstring")]
    http2: bool,
    #[serde(rename = "urlPath")]
    url_path: String,
    #[serde(rename = "httpReferer")]
    referer: String,
    #[serde(rename = "httpUA")]
    user_agent: String,
    #[serde(rename = "cacheState")]
    cache_state: String,
    #[serde(
        rename = "respStatus",
        deserialize_with = "deserialize_number_from_string"
    )]
    response_status: usize,
    #[serde(
        rename = "respTotalBytes",
        deserialize_with = "deserialize_number_from_string"
    )]
    response_bytes: usize,
    #[serde(
        rename = "timeElapsed",
        deserialize_with = "deserialize_duration_from_usec_string"
    )]
    response_duration: Duration,
    #[serde(rename = "reqStartTime", deserialize_with = "deserialize_start_time")]
    request_start_time: DateTime<Utc>,
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

// From serde_aux crate, under MIT license
fn deserialize_number_from_string<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr + serde::Deserialize<'de>,
    <T as FromStr>::Err: Display,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrInt<T> {
        String(String),
        Number(T),
    }

    match StringOrInt::<T>::deserialize(deserializer)? {
        StringOrInt::String(s) => s.parse::<T>().map_err(serde::de::Error::custom),
        StringOrInt::Number(i) => Ok(i),
    }
}

// From serde_aux crate, under MIT license
fn deserialize_duration_from_usec_string<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let number = deserialize_number_from_string(deserializer)?;
    Ok(Duration::from_micros(number))
}

// Based on serde_aux crate, under MIT license
fn deserialize_bool_from_bitstring<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringLike {
        String(String),
        Number(usize),
        Bool(bool),
    }

    let number = match StringLike::deserialize(deserializer)? {
        StringLike::String(s) => s.parse::<usize>().map_err(serde::de::Error::custom)?,
        StringLike::Number(i) => i,
        StringLike::Bool(b) => {
            if b {
                1
            } else {
                0
            }
        }
    };
    match number {
        0 => Ok(false),
        1 => Ok(true),
        i => Err(serde::de::Error::custom(format!(
            "expected boolean value, got a nonzero, non-one value: {i}"
        ))),
    }
}

/// Deserializes the start time.
/// In older logs, it was an RFC2822 string;
/// in newer ones, it's an epoch time.
fn deserialize_start_time<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringLike {
        String(String),
        Number(i64),
    }

    match StringLike::deserialize(deserializer)? {
        StringLike::Number(i) => {
            // Only at a 1 second granularity.
            DateTime::from_timestamp(i, 0)
                .ok_or("error in generating timestamp")
                .map_err(serde::de::Error::custom)
        }
        StringLike::String(s) => {
            if let Ok(v) = DateTime::<FixedOffset>::parse_from_rfc2822(&s) {
                Ok(v.into())
            } else if let Ok(v) = DateTime::<FixedOffset>::parse_from_rfc3339(&s) {
                Ok(v.into())
            } else {
                Err(serde::de::Error::custom(
                    "unknown string format for timestamp",
                ))
            }
        }
    }
}

impl LogEntry {
    /// Store this log entry as part of a transaction.
    ///
    /// We insert multiple objects as part of a single transaction to avoid duplicates;
    /// we consume an entire file (multiple records) at once.
    pub fn store(&self, tx: &Transaction) -> Result<(), rusqlite::Error> {
        let ipv4 = get_ipv4(&self.client_ip);
        let ipv6 = get_ipv6(&self.client_ip);
        let _ = tx
            .prepare_cached(
                "INSERT INTO client_ips (ipv4, ipv6) VALUES (?, ?) ON CONFLICT DO NOTHING;",
            )
            .unwrap()
            .execute([&ipv4, &ipv6])?;
        tx.prepare_cached("INSERT INTO paths (path) VALUES (?) ON CONFLICT DO NOTHING;")
            .unwrap()
            .execute([&self.url_path])?;
        tx.prepare_cached("INSERT INTO referers (referer) VALUES (?) ON CONFLICT DO NOTHING;")
            .unwrap()
            .execute([&self.referer])?;
        tx.prepare_cached(
            "INSERT INTO user_agents (user_agent) VALUES (?) ON CONFLICT DO NOTHING;",
        )
        .unwrap()
        .execute([&self.user_agent])?;
        tx.prepare_cached(
            r#"
INSERT INTO requests (
  client_ip
, asn
, country_code
, requests
, ipv6
, http2
, cache_state
, response_status
, response_bytes
, response_duration
, request_start_time
, url_path
, referer
, user_agent
) VALUES (
  ( SELECT id FROM client_ips WHERE ipv4 = :client_ipv4 OR ipv6 = :client_ipv6)
, :asn
, :country_code
, :requests
, :ipv6
, :http2
, :cache_state
, :response_status
, :response_bytes
, :response_duration
, :request_start_time
, ( SELECT id FROM paths WHERE path = :url_path)
, ( SELECT id FROM referers WHERE referer = :referer)
, ( SELECT id FROM user_agents WHERE user_agent = :user_agent)
);"#,
        )?
        .execute(named_params! {
            ":client_ipv4": &ipv4,
            ":client_ipv6": &ipv6,
            ":asn": self.asn as usize,
            ":country_code": &self.country_code,
            ":requests": self.requests,
            ":ipv6": self.ipv6,
            ":http2": self.http2,
            ":cache_state": &self.cache_state,
            ":response_bytes": self.response_bytes,
            ":response_status": self.response_status,
            ":response_duration": self.response_duration.as_secs_f32(),
            ":request_start_time": &self.request_start_time.to_rfc3339(),
            ":url_path": &self.url_path,
            ":user_agent": &self.user_agent,
            ":referer": &self.referer,
        })
        .map(|_| ())
    }
}
