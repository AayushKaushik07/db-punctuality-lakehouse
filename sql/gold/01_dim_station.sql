-- dim_station: one row per station (EVA number) with a UNIQUE display name.
--
-- Big stations have separate EVA numbers for S-Bahn or lower-level platforms, but share
-- the same `station_name` (e.g. three EVAs are all "Berlin Hauptbahnhof"). Naming rule:
--   1. the most frequent `station_name` for the EVA;
--   2. if another EVA has that name too: the more specific `xml_station_name`
--      (e.g. "Berlin Hbf (S-Bahn)");
--   3. if that still isn't unique: append the EVA number.
WITH name_counts AS (
    SELECT eva, station_name, xml_station_name, COUNT(*) AS n
    FROM silver_stop_events
    GROUP BY eva, station_name, xml_station_name
),
preferred AS (
    SELECT
        eva,
        station_name,
        COALESCE(xml_station_name, station_name) AS detail_name,
        ROW_NUMBER() OVER (PARTITION BY eva ORDER BY n DESC, station_name) AS rn
    FROM name_counts
),
step1 AS (
    SELECT
        eva,
        station_name,
        detail_name,
        COUNT(*) OVER (PARTITION BY station_name) AS evas_with_name
    FROM preferred
    WHERE rn = 1
),
step2 AS (
    SELECT
        eva,
        station_name AS base_name,
        CASE WHEN evas_with_name > 1 THEN detail_name ELSE station_name END AS candidate
    FROM step1
),
named AS (
    SELECT
        eva,
        base_name,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY candidate) > 1 THEN CONCAT(candidate, ' [', eva, ']')
            ELSE candidate
        END AS station_name
    FROM step2
),
activity AS (
    SELECT
        eva,
        MIN(service_date)          AS first_seen_date,
        MAX(service_date)          AS last_seen_date,
        COUNT(*)                   AS stop_events,
        COUNT(DISTINCT ride_id)    AS journeys,
        COUNT(DISTINCT train_type) AS train_types
    FROM silver_stop_events
    GROUP BY eva
)
SELECT
    n.eva,
    n.station_name,
    n.base_name,
    a.first_seen_date,
    a.last_seen_date,
    a.stop_events,
    a.journeys,
    a.train_types
FROM named AS n
JOIN activity AS a USING (eva)
