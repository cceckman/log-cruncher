CREATE TEMP VIEW reqs AS
SELECT
    requests.response_status as status
,   requests.client_ip as client_ip
,   requests.ipv6 as ipv6
,   requests.http2 as http2
,   requests.asn as client_asn
,   autonomous_systems.name as asn_name
,   requests.cache_state as cache_state
,   requests.response_bytes as size
,   requests.request_start_time as time -- in RFC3339 format
,   requests.response_duration as duration
,   paths.path as url_path
,   referers.referer as referer
,   user_agents.user_agent as user_agent
,   date(requests.request_start_time) as date
FROM
    requests
    -- We don't have the same column name in the two tables,
    -- so we can't NATURAL JOIN. Oh well.
    LEFT JOIN paths ON requests.url_path = paths.id
    LEFT JOIN referers ON requests.referer = referers.id
    LEFT JOIN user_agents ON requests.user_agent = user_agents.id
    LEFT JOIN autonomous_systems ON requests.asn = autonomous_systems.asn
;

-- without blackbox probes...
CREATE TEMP VIEW alltime_allreq AS
SELECT * FROM reqs
WHERE user_agent NOT LIKE "%blackbox%";

-- ...and without spam traffic, where we can get rid of it.
CREATE TEMP VIEW alltime AS
SELECT * FROM alltime_allreq
WHERE status != 404;

-- Just the last week
CREATE TEMP VIEW r AS
SELECT * FROM alltime
WHERE time > datetime("now", "-7 days");
-- The above is not quite right; we have a T in the database
-- (in older entries) but this doesn't.
-- Fixed by having newer entries run via datetime(), so everything is
-- canonicalized to sqlite's version.

