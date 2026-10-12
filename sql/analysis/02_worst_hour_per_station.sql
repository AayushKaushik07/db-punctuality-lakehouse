-- For every big station: which hour of the day is the worst for punctuality?
-- ROW_NUMBER() picks the top row per group (a classic "top-N per group" pattern).
--
-- Run:  python -m dbpipe query sql/analysis/02_worst_hour_per_station.sql
WITH station_hour AS (
    SELECT
        eva,
        event_hour,
        COUNT(*)                                                   AS arrivals,
        100.0D * AVG(CASE WHEN is_arrival_punctual THEN 0 ELSE 1 END) AS late_pct
    FROM gold_fct_stop_events
    WHERE is_arrival_measured
      AND train_category IN ('long_distance', 'regional', 's_bahn')
    GROUP BY eva, event_hour
    HAVING COUNT(*) >= 100
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY eva ORDER BY late_pct DESC)       AS worst_rank,
        AVG(late_pct) OVER (PARTITION BY eva)                             AS station_avg_late_pct
    FROM station_hour
)
SELECT
    s.station_name,
    r.event_hour                                        AS worst_hour,
    r.arrivals,
    ROUND(r.late_pct, 1)                                AS late_pct_in_worst_hour,
    ROUND(r.station_avg_late_pct, 1)                    AS station_avg_late_pct,
    ROUND(r.late_pct - r.station_avg_late_pct, 1)       AS excess_pp
FROM ranked AS r
JOIN gold_dim_station AS s USING (eva)
WHERE r.worst_rank = 1
ORDER BY excess_pp DESC
