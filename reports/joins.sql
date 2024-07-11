CREATE TEMP TABLE reqs AS
SELECT
    requests.response_status as status
,   requests.asn as client_asn
,   requests.cache_state as cache_state
,   requests.response_bytes as size
,   requests.request_start_time as time
,   requests.response_duration as duration
,   paths.path as url_path
,   referers.referer as referer
,   user_agents.user_agent as user_agent
FROM
    requests
    LEFT JOIN paths ON requests.url_path = paths.id
    LEFT JOIN referers ON requests.referer = referers.id
    LEFT JOIN user_agents ON requests.user_agent = user_agents.id
;

-- and without.
CREATE TEMP TABLE alltime AS
SELECT * FROM reqs
WHERE reqs.user_agent NOT LIKE "%blackbox%";

-- Just the last week
CREATE TEMP TABLE r AS
SELECT * FROM alltime
WHERE time >= (unixepoch("now") - (60 * 60 * 24 * 7));



