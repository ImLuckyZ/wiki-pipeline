-- Queries that answer the three dashboard questions.
-- Table: edits_per_minute(window_start, wiki, is_bot, edit_count, bytes_changed)

-- 1) Activity over time: edits per minute, last hour
SELECT window_start, SUM(edit_count) AS edits
FROM edits_per_minute
WHERE window_start >= now() - interval '60 minutes'
GROUP BY window_start
ORDER BY window_start;

-- 1b) How activity changes by hour of day (needs several hours of data)
SELECT EXTRACT(HOUR FROM window_start AT TIME ZONE 'UTC') AS hour_utc,
       ROUND(AVG(per_minute), 1) AS avg_edits_per_minute
FROM (
    SELECT window_start, SUM(edit_count) AS per_minute
    FROM edits_per_minute
    GROUP BY window_start
) t
GROUP BY hour_utc
ORDER BY hour_utc;

-- 2) Most active wikis, last 10 minutes (window function for ranking)
SELECT wiki, total_edits,
       RANK() OVER (ORDER BY total_edits DESC) AS rnk
FROM (
    SELECT wiki, SUM(edit_count) AS total_edits
    FROM edits_per_minute
    WHERE window_start >= now() - interval '10 minutes'
    GROUP BY wiki
) t
ORDER BY rnk
LIMIT 10;

-- 3) Bot share of edits, last hour (overall)
SELECT ROUND(
         100.0 * SUM(edit_count) FILTER (WHERE is_bot)
         / NULLIF(SUM(edit_count), 0), 1) AS bot_percent
FROM edits_per_minute
WHERE window_start >= now() - interval '60 minutes';

-- 3b) Bot share per wiki (only wikis with enough edits to be meaningful)
SELECT wiki,
       SUM(edit_count) AS total_edits,
       ROUND(100.0 * SUM(edit_count) FILTER (WHERE is_bot)
             / NULLIF(SUM(edit_count), 0), 1) AS bot_percent
FROM edits_per_minute
WHERE window_start >= now() - interval '60 minutes'
GROUP BY wiki
HAVING SUM(edit_count) >= 100
ORDER BY bot_percent DESC;
