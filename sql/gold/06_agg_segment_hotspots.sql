-- agg_segment_hotspots: where do trains LOSE time?
-- For every pair of consecutive (covered) stations on a journey we compare the departure
-- delay at the previous station with the arrival delay at this station.
-- delay_gain_min > 0 means the train got later on that stretch.
--
-- How segments are ranked (lessons from the real data):
--  * By minutes lost PER SCHEDULED HOUR, not per segment: segments range from 5 minutes
--    to 3.5 hours (stations in between are not in the dataset), and longer stretches
--    simply have more time to lose minutes (share losing 3+ min vs. length: r = 0.56).
--  * By the MEDIAN, not the average: a few trains stuck for hours can push a segment's
--    average to +16 min while its median is 0.
--  * Only segments scheduled for at least ${min_segment_planned_min} minutes: delays are
--    whole minutes, so +1 min on a 5-minute hop would already count as 12 min per hour.
WITH segments AS (
    SELECT
        prev_eva,
        eva,
        train_category,
        delay_gain_min,
        segment_planned_min,
        delay_gain_min * 60.0D / segment_planned_min AS gain_per_hour
    FROM gold_fct_stop_events
    WHERE delay_gain_min IS NOT NULL
      AND prev_eva IS NOT NULL
      AND prev_eva <> eva
      AND segment_planned_min > 0
      AND train_category IN ('long_distance', 'regional', 's_bahn')
),
stats AS (
    SELECT
        prev_eva,
        eva,
        COUNT(*)                                                    AS observations,
        PERCENTILE_APPROX(segment_planned_min, 0.5)                 AS median_planned_min,
        PERCENTILE_APPROX(gain_per_hour, 0.5)                       AS median_gain_per_hour,
        PERCENTILE_APPROX(delay_gain_min, 0.5)                      AS median_delay_gain_min,
        PERCENTILE_APPROX(delay_gain_min, 0.9)                      AS p90_delay_gain_min,
        AVG(delay_gain_min)                                         AS avg_delay_gain_min,
        AVG(CASE WHEN delay_gain_min >= 3 THEN 1.0D ELSE 0.0D END)  AS share_gaining_3min,
        -- the most common train category on this segment
        MODE(train_category)                                        AS main_train_category
    FROM segments
    GROUP BY prev_eva, eva
    HAVING COUNT(*) >= ${min_segment_observations}
       AND PERCENTILE_APPROX(segment_planned_min, 0.5) >= ${min_segment_planned_min}
)
SELECT
    st.prev_eva                             AS from_eva,
    a.station_name                          AS from_station,
    st.eva                                  AS to_eva,
    b.station_name                          AS to_station,
    st.main_train_category,
    st.observations,
    st.median_planned_min,
    ROUND(st.median_gain_per_hour, 1)       AS median_gain_per_hour,
    st.median_delay_gain_min,
    ROUND(100 * st.share_gaining_3min, 2)   AS pct_trains_gaining_3min,
    st.p90_delay_gain_min,
    ROUND(st.avg_delay_gain_min, 2)         AS avg_delay_gain_min,
    RANK() OVER (ORDER BY st.median_gain_per_hour DESC, st.share_gaining_3min DESC) AS hotspot_rank
FROM stats AS st
LEFT JOIN gold_dim_station AS a ON st.prev_eva = a.eva
LEFT JOIN gold_dim_station AS b ON st.eva = b.eva
