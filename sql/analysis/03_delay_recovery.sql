-- Do late departures turn into late arrivals, or do trains recover time?
-- For each ride: delay when leaving the first covered station vs. arriving at the last one.
-- FIRST_VALUE / LAST_VALUE over the ride, ordered by stop sequence.
--
-- Run:  python -m dbpipe query sql/analysis/03_delay_recovery.sql
WITH ride_stops AS (
    SELECT
        ride_id,
        train_category,
        FIRST_VALUE(departure_delay_min) OVER w  AS first_departure_delay,
        LAST_VALUE(arrival_delay_min) OVER w     AS last_arrival_delay,
        COUNT(*) OVER w                          AS covered_stops
    FROM gold_fct_stop_events
    WHERE NOT is_canceled
      AND train_category IN ('long_distance', 'regional', 's_bahn')
    WINDOW w AS (PARTITION BY ride_id ORDER BY stop_seq
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
rides AS (
    SELECT DISTINCT ride_id, train_category, first_departure_delay, last_arrival_delay
    FROM ride_stops
    WHERE covered_stops >= 2
      AND first_departure_delay IS NOT NULL
      AND last_arrival_delay IS NOT NULL
)
SELECT
    train_category,
    CASE
        WHEN first_departure_delay < 3  THEN '1: left on time (<3 min)'
        WHEN first_departure_delay < 6  THEN '2: left 3-5 min late'
        WHEN first_departure_delay < 16 THEN '3: left 6-15 min late'
        ELSE                                 '4: left 16+ min late'
    END                                                                   AS departure_status,
    COUNT(*)                                                              AS rides,
    ROUND(AVG(last_arrival_delay - first_departure_delay), 2)             AS avg_change_min,
    ROUND(100.0D * AVG(CASE WHEN last_arrival_delay < 6 THEN 1 ELSE 0 END), 1) AS pct_punctual_at_end,
    ROUND(100.0D * AVG(CASE WHEN last_arrival_delay < first_departure_delay THEN 1 ELSE 0 END), 1)
                                                                          AS pct_recovered_time
FROM rides
GROUP BY train_category, 2
ORDER BY train_category, departure_status
