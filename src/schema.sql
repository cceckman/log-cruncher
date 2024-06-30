CREATE TABLE IF NOT EXISTS client_ips (
  id INTEGER PRIMARY KEY NOT NULL,
  ipv4 TEXT NULL UNIQUE,
  ipv6 TEXT NULL UNIQUE,
) STRICT;

CREATE TABLE IF NOT EXISTS paths (
  id INTEGER PRIMARY KEY NOT NULL,
  path TEXT NOT NULL UNIQUE,
) STRICT;

CREATE TABLE IF NOT EXISTS referers (
  id INTEGER PRIMARY KEY NOT NULL,
  referer TEXT NOT NULL UNIQUE,
) STRICT;

CREATE TABLE IF NOT EXISTS user_agents (
  id INTEGER PRIMARY KEY NOT NULL,
  user_agent TEXT NOT NULL UNIQUE,
) STRICT;

CREATE TABLE IF NOT EXISTS requests (
  id INTEGER PRIMARY KEY NOT NULL,
  client_ip INTEGER,
  asn INTEGER,
  country_code TEXT,
  requests INTEGER,
  ipv6 INTEGER,
  http2 INTEGER,
  cache_state TEXT,
  response_bytes INTEGER,
  response_duration: NUMBER,
  request_start_time: TEXT,
  url_path INTEGER NOT NULL,
  referer INTEGER,
  user_agent INTEGER,

  FOREIGN KEY(client_ip) REFERENCES client_ips(id);
  FOREIGN KEY(url_path) REFERENCES paths(id);
  FOREIGN KEY(referer) REFERENCES referers(id);
  FOREIGN KEY(user_agent) REFERENCES user_agents(id);
) STRICT;


