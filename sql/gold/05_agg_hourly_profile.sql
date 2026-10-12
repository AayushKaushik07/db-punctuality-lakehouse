-- agg_hourly_profile: how punctuality changes over the day and the week.
-- weekday: 0 = Monday ... 6 = Sunday
SELECT
    CASE WHEN GROUPING(train_category) = 1 THEN 'all_rail' ELSE train_category END AS train_category,
    weekday,
    event_hour,
    COUNT(*)                                                                    AS scheduled_stops,
    SUM(CAST(is_arrival_measured AS INT))                                       AS measured_arrivals,
    ROUND(100.0D * SUM(CAST(is_arrival_punctual AS INT))
          / NULLIF(SUM(CAST(is_arrival_measured AS INT)), 0), 2)               AS punctuality_pct,
    ROUND(AVG(CASE WHEN is_arrival_measured THEN arrival_delay_min END), 2)     AS avg_arrival_delay_min,
    ROUND(100.0D * SUM(CAST(is_canceled AS INT)) / COUNT(*), 2)                  AS cancellation_pct
FROM gold_fct_stop_events
WHERE train_category IN ('long_distance', 'regional', 's_bahn')
GROUP BY GROUPING SETS ((train_category, weekday, event_hour), (weekday, event_hour))
