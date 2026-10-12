-- agg_delay_distribution: how late are trains, in buckets? (for the dashboard histogram)
SELECT
    train_category,
    CASE
        WHEN arrival_delay_min < 0   THEN '0: early'
        WHEN arrival_delay_min < 3   THEN '1: 0-2 min'
        WHEN arrival_delay_min < 6   THEN '2: 3-5 min'
        WHEN arrival_delay_min < 16  THEN '3: 6-15 min'
        WHEN arrival_delay_min < 31  THEN '4: 16-30 min'
        WHEN arrival_delay_min < 61  THEN '5: 31-60 min'
        ELSE                              '6: over 60 min'
    END                                                   AS delay_bucket,
    COUNT(*)                                              AS arrivals,
    ROUND(100.0D * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY train_category), 2) AS pct_of_category
FROM gold_fct_stop_events
WHERE is_arrival_measured
  AND train_category IN ('long_distance', 'regional', 's_bahn')
GROUP BY train_category, 2
