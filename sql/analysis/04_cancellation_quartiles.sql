-- Split stations into quartiles by cancellation rate (NTILE) and compare the groups.
-- Question: are stations with many cancellations also the ones with the worst delays?
--
-- Run:  python -m dbpipe query sql/analysis/04_cancellation_quartiles.sql
WITH station AS (
    SELECT
        eva,
        COUNT(*)                                                           AS scheduled_stops,
        100.0D * AVG(CAST(is_canceled AS INT))                              AS cancellation_pct,
        100.0D * SUM(CAST(is_arrival_punctual AS INT))
              / NULLIF(SUM(CAST(is_arrival_measured AS INT)), 0)            AS punctuality_pct
    FROM gold_fct_stop_events
    WHERE train_category IN ('long_distance', 'regional', 's_bahn')
    GROUP BY eva
    HAVING COUNT(*) >= ${min_station_arrivals}
),
quartiles AS (
    SELECT *, NTILE(4) OVER (ORDER BY cancellation_pct) AS cancellation_quartile
    FROM station
)
SELECT
    cancellation_quartile,
    COUNT(*)                              AS stations,
    ROUND(MIN(cancellation_pct), 2)       AS min_cancellation_pct,
    ROUND(MAX(cancellation_pct), 2)       AS max_cancellation_pct,
    ROUND(AVG(punctuality_pct), 1)        AS avg_punctuality_pct
FROM quartiles
GROUP BY cancellation_quartile
ORDER BY cancellation_quartile
