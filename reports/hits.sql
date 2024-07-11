.read reports/joins.sql

SELECT COUNT(*) as traffic FROM r;

-- Top user agents
SELECT substr(r.user_agent, 0, 70) top_agents, COUNT(*) as count
FROM r
GROUP BY r.user_agent
ORDER BY count DESC
LIMIT 20;

-- Top referers
SELECT substr(r.referer, 0, 70) as top_referer, COUNT(*) as count
FROM r
WHERE
    r.referer NOT NULL
AND r.referer NOT LIKE "%cceckman.com%"
GROUP BY r.referer
ORDER BY count DESC
LIMIT 20;

-- Top pages
SELECT substr(r.url_path, 0, 70) as top_page, COUNT(*) as count
FROM r
GROUP BY r.url_path
ORDER BY count DESC
LIMIT 20;

-- Top articles
SELECT substr(r.url_path, 0, 70) as top_articles, COUNT(*) as count
FROM r
WHERE r.url_path LIKE "%/writing/%/"
GROUP BY r.url_path
ORDER BY count DESC
LIMIT 20;

-- Top errors
SELECT r.status, substr(r.url_path, 0, 70) as top_articles, COUNT(*) as count
FROM r
WHERE r.status >= 400
-- ...ignoring common probes:
    AND NOT r.url_path LIKE '/wp%'
    AND NOT r.url_path LIKE '%.php'
GROUP BY r.status, r.url_path
ORDER BY count DESC;

