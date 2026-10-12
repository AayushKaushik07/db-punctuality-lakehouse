-- agg_punctuality_daily: daily KPIs per train category plus an 'all_rail' total.
-- GROUPING SETS computes the per-category rows and the total in one pass.
-- Buses (rail replacement) and unknown train types are excluded from all KPIs.
WITH base AS (
    SELECT *
    FROM gold_fct_stop_events
    WHERE train_category IN ('long_distance', 'regional', 's_bahn')
),
daily AS (
    SELECT
        service_date,
        CASE WHEN GROUPING(train_category) = 1 THEN 'all_rail' ELSE train_category END AS train_category,
        COUNT(*)                                                          AS scheduled_stops,
        SUM(CAST(is_arrival_measured AS INT))                             AS measured_arrivals,
        SUM(CAST(is_arrival_punctual AS INT))                             AS punctual_arrivals,
        SUM(CAST(is_canceled AS INT))                                     AS canceled_stops,
        AVG(CASE WHEN is_arrival_measured THEN arrival_delay_min END)     AS avg_arrival_delay_min,
        PERCENTILE_APPROX(CASE WHEN is_arrival_measured THEN arrival_delay_min END, 0.9)
                                                                          AS p90_arrival_delay_min
    FROM base
    GROUP BY GROUPING SETS ((service_date, train_category), (service_date))
),
-- Distinct rides are counted in a separate step on purpose: COUNT(DISTINCT ...) next to
-- PERCENTILE_APPROX in one GROUP BY makes Spark merge a percentile sketch per distinct
-- ride, which took over 10 minutes on two real months instead of seconds.
daily_rides AS (
    SELECT
        service_date,
        CASE WHEN GROUPING(train_category) = 1 THEN 'all_rail' ELSE train_category END AS train_category,
        COUNT(DISTINCT ride_id) AS rides
    FROM base
    GROUP BY GROUPING SETS ((service_date, train_category), (service_date))
)
SELECT
    d.service_date,
    d.train_category,
    d.scheduled_stops,
    d.measured_arrivals,
    d.punctual_arrivals,
    d.canceled_stops,
    r.rides,
    ROUND(100.0D * d.punctual_arrivals / NULLIF(d.measured_arrivals, 0), 2) AS punctuality_pct,
    ROUND(100.0D * d.canceled_stops / NULLIF(d.scheduled_stops, 0), 2)      AS cancellation_pct,
    ROUND(d.avg_arrival_delay_min, 2)                                       AS avg_arrival_delay_min,
    d.p90_arrival_delay_min,
    dt.is_weekend,
    dt.is_public_holiday,
    dt.holiday_name,
    -- 7-day moving average smooths out the weekday/weekend pattern
    ROUND(AVG(100.0D * d.punctual_arrivals / NULLIF(d.measured_arrivals, 0)) OVER (
        PARTITION BY d.train_category
        ORDER BY d.service_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                                   AS punctuality_pct_7d_avg
FROM daily AS d
JOIN daily_rides AS r
    ON d.service_date = r.service_date AND d.train_category = r.train_category
LEFT JOIN gold_dim_date AS dt
    ON d.service_date = dt.date
