-- agg_official_comparison: validate our numbers against Deutsche Bahn's published
-- punctuality (reference/db_official_punctuality.csv: monthly values and half-year values).
--
-- For every official period we pool OUR arrivals over the same months and compare.
-- A period is only compared when all of its months were processed.
--
-- Expect differences, and explain them rather than hide them:
--   * until 2025-10 the dataset only covers the ~100 biggest stations (big hubs are
--     usually less punctual than small stations)
--   * the dataset includes non-DB operators (e.g. private regional operators, FLX)
--   * DB measures at every stop in Germany with internal operational data
WITH ours_monthly AS (
    SELECT
        source_month AS month,
        CASE WHEN train_category = 'long_distance' THEN 'fernverkehr' ELSE 'regio' END AS segment,
        SUM(CAST(is_arrival_measured AS INT)) AS measured,
        SUM(CAST(is_arrival_punctual AS INT)) AS punctual
    FROM gold_fct_stop_events
    WHERE train_category IN ('long_distance', 'regional', 's_bahn')
    GROUP BY 1, 2
),
ours_with_total AS (
    SELECT month, segment, measured, punctual FROM ours_monthly
    UNION ALL
    SELECT month, 'gesamt' AS segment, SUM(measured), SUM(punctual) FROM ours_monthly GROUP BY month
),
official AS (
    SELECT period, first_month, last_month, source, segment, official_pct
    FROM ref_official_punctuality
    LATERAL VIEW STACK(
        3,
        'fernverkehr', fernverkehr_pct,
        'regio',       regio_pct,
        'gesamt',      personenverkehr_gesamt_pct
    ) AS segment, official_pct
),
pooled AS (
    SELECT
        r.period,
        r.first_month,
        r.source,
        r.segment,
        r.official_pct,
        -- number of months the period should have vs. how many we processed
        MONTHS_BETWEEN(TO_DATE(CONCAT(r.last_month, '-01')), TO_DATE(CONCAT(r.first_month, '-01'))) + 1
                                                     AS expected_months,
        COUNT(DISTINCT o.month)                      AS covered_months,
        SUM(o.measured)                              AS measured_arrivals,
        100.0D * SUM(o.punctual) / NULLIF(SUM(o.measured), 0) AS our_punctuality_pct
    FROM official AS r
    JOIN ours_with_total AS o
        ON o.segment = r.segment
       AND o.month BETWEEN r.first_month AND r.last_month
    GROUP BY r.period, r.first_month, r.last_month, r.source, r.segment, r.official_pct
)
SELECT
    period,
    first_month,
    CASE WHEN expected_months > 1 THEN 'half_year' ELSE 'month' END AS period_type,
    segment,
    source,
    measured_arrivals,
    ROUND(our_punctuality_pct, 2)                  AS our_punctuality_pct,
    official_pct                                   AS official_punctuality_pct,
    ROUND(our_punctuality_pct - official_pct, 2)   AS difference_pp
FROM pooled
WHERE covered_months = expected_months
