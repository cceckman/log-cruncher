
.read joins.sql

.print 'From the last week...'

.print ''
.print 'Top articles:'
SELECT substr(r.url_path, 0, 70) as top_articles, COUNT(*) as count
FROM r
WHERE r.url_path LIKE "%/writing/%/"
GROUP BY r.url_path
ORDER BY count DESC
LIMIT 20;

.print ''
.print 'Top 3 articles by date:'
WITH counts_by_date AS (
    -- Sub-select: count of article-visits by date
    SELECT 
        substr(r.url_path, 0, 70) as top_articles
    ,   COUNT(*) as count
    ,   date
    FROM r
    WHERE r.url_path LIKE "%/writing/%/"
    GROUP BY date, url_path
)
SELECT top_articles, count, date FROM (
    SELECT *, row_number() over win1 as url_rank
    FROM counts_by_date
    WINDOW win1 AS (PARTITION BY date ORDER BY count DESC)
)
WHERE url_rank <= 3
ORDER BY date DESC, count DESC;

.print ''
.print 'Top referers:'
SELECT substr(r.referer, 0, 70) as top_referer, COUNT(*) as count
FROM r
WHERE
    r.referer NOT NULL
AND r.referer != ""
AND r.referer NOT LIKE "%cceckman.com%"
GROUP BY r.referer
ORDER BY count DESC
LIMIT 20;

.print ''
.print 'Top user agents:'
SELECT substr(r.user_agent, 0, 70) top_agents, COUNT(*) as count
FROM r
GROUP BY r.user_agent
ORDER BY count DESC
LIMIT 20;

.print ''
.print 'Top pages:'
SELECT substr(r.url_path, 0, 70) as top_page, COUNT(*) as count
FROM r
GROUP BY r.url_path
ORDER BY count DESC
LIMIT 20;



.print ''
.print 'Top errors:'
SELECT r.status, substr(r.url_path, 0, 70) as top_errors, COUNT(*) as count
FROM r
WHERE r.status >= 400
-- ...ignoring common vulnerability scanners:
    AND NOT r.url_path LIKE '/wp%'
    AND NOT r.url_path LIKE '%.php'
GROUP BY r.status, r.url_path
ORDER BY count DESC
LIMIT 20;

.print ''
.print 'Top scanning ASNs:'
SELECT
    client_asn
,   asn_name
,   COUNT(*) as count
FROM r
WHERE
    url_path LIKE '%.php'
OR  url_path LIKE '/wp-%'
GROUP BY client_asn
ORDER BY count DESC
LIMIT 20;

