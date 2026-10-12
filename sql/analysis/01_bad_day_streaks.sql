-- Gaps-and-islands: find streaks of consecutive "bad days" per train category.
-- A bad day = punctuality in the worst 25% of days for that category.
-- Trick: for consecutive dates, (date - row_number) is constant -> use it as the streak id.
--
-- Run:  python -m dbpipe query sql/analysis/01_bad_day_streaks.sql
WITH daily AS (
    SELECT service_date, train_category, punctuality_pct
    FROM gold_agg_punctuality_daily
    WHERE train_category IN ('long_distance', 'regional', 's_bahn')
),
thresholds AS (
    SELECT train_category, PERCENTILE_APPROX(punctuality_pct, 0.25) AS p25
    FROM daily
    GROUP BY train_category
),
bad_days AS (
    SELECT
        d.train_category,
        d.service_date,
        d.punctuality_pct,
        DATE_SUB(d.service_date,
                 CAST(ROW_NUMBER() OVER (PARTITION BY d.train_category ORDER BY d.service_date) AS INT)) AS streak_id
    FROM daily AS d
    JOIN thresholds AS t USING (train_category)
    WHERE d.punctuality_pct <= t.p25
)
SELECT
    train_category,
    MIN(service_date)               AS streak_start,
    MAX(service_date)               AS streak_end,
    COUNT(*)                        AS days,
    ROUND(AVG(punctuality_pct), 1)  AS avg_punctuality_pct
FROM bad_days
GROUP BY train_category, streak_id
HAVING COUNT(*) >= 2
ORDER BY days DESC, avg_punctuality_pct
