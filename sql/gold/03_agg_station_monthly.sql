-- agg_station_monthly: station ranking per month.
-- Rankings only include stations with enough arrivals to be meaningful;
-- LAG() gives the change versus the previous month for the same station.
WITH station_month AS (
    SELECT
        f.source_month,
        f.eva,
        COUNT(*)                                                      AS scheduled_stops,
        SUM(CAST(f.is_arrival_measured AS INT))                       AS measured_arrivals,
        SUM(CAST(f.is_arrival_punctual AS INT))                       AS punctual_arrivals,
        SUM(CAST(f.is_canceled AS INT))                               AS canceled_stops,
        AVG(CASE WHEN f.is_arrival_measured THEN f.arrival_delay_min END) AS avg_arrival_delay_min,
        SUM(CASE WHEN f.is_arrival_measured AND f.arrival_delay_min >= 15 THEN 1 ELSE 0 END)
                                                                      AS arrivals_15min_late
    FROM gold_fct_stop_events AS f
    WHERE f.train_category IN ('long_distance', 'regional', 's_bahn')
    GROUP BY f.source_month, f.eva
),
kpis AS (
    SELECT
        *,
        100.0D * punctual_arrivals / NULLIF(measured_arrivals, 0)   AS punctuality_pct,
        100.0D * canceled_stops / NULLIF(scheduled_stops, 0)        AS cancellation_pct,
        100.0D * arrivals_15min_late / NULLIF(measured_arrivals, 0) AS late_15min_pct,
        measured_arrivals >= ${min_station_arrivals}               AS is_rankable
    FROM station_month
)
SELECT
    k.source_month,
    k.eva,
    s.station_name,
    k.scheduled_stops,
    k.measured_arrivals,
    ROUND(k.punctuality_pct, 2)        AS punctuality_pct,
    ROUND(k.cancellation_pct, 2)       AS cancellation_pct,
    ROUND(k.late_15min_pct, 2)         AS late_15min_pct,
    ROUND(k.avg_arrival_delay_min, 2)  AS avg_arrival_delay_min,
    k.is_rankable,
    CASE WHEN k.is_rankable THEN
        RANK() OVER (PARTITION BY k.source_month, k.is_rankable ORDER BY k.punctuality_pct ASC)
    END                                AS worst_punctuality_rank,
    ROUND(k.punctuality_pct - LAG(k.punctuality_pct) OVER (
        PARTITION BY k.eva ORDER BY k.source_month
    ), 2)                              AS punctuality_change_pp
FROM kpis AS k
LEFT JOIN gold_dim_station AS s
    ON k.eva = s.eva
