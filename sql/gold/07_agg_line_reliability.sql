-- agg_line_reliability: which lines / trains are the least reliable?
-- Long-distance trains have no line number in the source data, so they are
-- identified by their train label (e.g. "ICE 123"). Regional line numbers are not
-- unique across Germany (several regions have an "RE1"), so the number of distinct
-- stations served is included to help spot mixed lines.
WITH keyed AS (
    SELECT
        *,
        CASE
            WHEN line_number IS NOT NULL THEN CONCAT(train_type, ' | ', line_number)
            ELSE train_label
        END AS route_key
    FROM gold_fct_stop_events
    WHERE train_category IN ('long_distance', 'regional', 's_bahn')
),
per_route AS (
    SELECT
        route_key,
        -- route_key contains the train type, so the category is the same on every row
        MAX(train_category)                                               AS train_category,
        COUNT(DISTINCT ride_id)                                           AS rides,
        COUNT(DISTINCT eva)                                               AS stations_served,
        SUM(CAST(is_arrival_measured AS INT))                             AS measured_arrivals,
        SUM(CAST(is_arrival_punctual AS INT))                             AS punctual_arrivals,
        SUM(CAST(is_canceled AS INT))                                     AS canceled_stops,
        COUNT(*)                                                          AS scheduled_stops,
        AVG(CASE WHEN is_arrival_measured THEN arrival_delay_min END)     AS avg_arrival_delay_min,
        MAX(CASE WHEN is_arrival_measured THEN arrival_delay_min END)     AS max_arrival_delay_min
    FROM keyed
    GROUP BY route_key
    HAVING COUNT(DISTINCT ride_id) >= 20
)
SELECT
    route_key,
    train_category,
    rides,
    stations_served,
    measured_arrivals,
    ROUND(100.0D * punctual_arrivals / NULLIF(measured_arrivals, 0), 2) AS punctuality_pct,
    ROUND(100.0D * canceled_stops / NULLIF(scheduled_stops, 0), 2)      AS cancellation_pct,
    ROUND(avg_arrival_delay_min, 2)                                     AS avg_arrival_delay_min,
    max_arrival_delay_min,
    -- rank within its own category, worst first
    RANK() OVER (
        PARTITION BY train_category
        ORDER BY punctual_arrivals / NULLIF(measured_arrivals, 0) ASC
    )                                                                   AS worst_rank_in_category
FROM per_route
