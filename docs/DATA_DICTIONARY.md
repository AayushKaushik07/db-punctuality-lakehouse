# Data dictionary

Conventions: `*_pct` = percentage 0–100 · `*_min` = minutes · `eva` = Deutsche Bahn's station
number · `weekday` 0 = Monday … 6 = Sunday · `train_category` ∈ `long_distance`, `regional`,
`s_bahn`, `bus` (plus `all_rail` for totals in aggregates).

## Source (bronze)

`bronze.stop_events`: the monthly files after the schema contract, plus lineage columns.
Source columns are documented in the
[dataset README](https://github.com/piebro/deutsche-bahn-data#monthly-release-data-schema).

| Added column | Meaning |
|---|---|
| `source_month` | month of the file the row came from (partition key) |
| `_source_file`, `_source_sha256` | exact file + checksum the row was loaded from |
| `_ingested_at` | load time (UTC) |

`bronze._ingestion_log`: one row per load: month, checksum, bytes, row count, schema notes.

## Silver

`silver.stop_events`: one row per unique stop event, cleaned.

| Column | Meaning |
|---|---|
| `id` | unique stop id from the source |
| `service_date`, `event_hour`, `weekday` | derived from the planned arrival (or departure for origins), Berlin time |
| `eva`, `station_name` | station |
| `train_type`, `train_number`, `line_number`, `train_label` | e.g. `ICE`, `123`, `NULL`, `ICE 123` |
| `ride_id` | one **journey**: one train on one day, taken from the stop id (`<pattern>-<trip start>-<stop no>` → `<pattern>-<trip start>`) |
| `line_ride_id` | the recurring timetable pattern (the source's `train_line_ride_id`, identical on every day the train runs) |
| `stop_seq` | position of the stop in the journey |
| `xml_station_name` | the source's more specific station name (e.g. `Berlin Hbf (S-Bahn)`) |
| `stop_role` | `origin` (no arrival), `terminus` (no departure) or `intermediate` |
| `arrival_planned_ts`, `arrival_actual_ts`, `departure_planned_ts`, `departure_actual_ts` | actual = changed time, or planned if no change was reported |
| `arrival_delay_min`, `departure_delay_min` | recomputed from the timestamps |
| `source_delay_min`, `delay_matches_source` | the dataset's own `delay_in_min` and whether it agrees (±1 min) |
| `arrival_is_canceled`, `departure_is_canceled`, `is_canceled` | cancellation flags |
| `prev_eva`, `prev_station_name` | previous covered stop of the same journey |
| `segment_planned_min` | scheduled minutes from departure at the previous covered stop to arrival here |
| `delay_gain_min` | arrival delay here minus departure delay at the previous stop |

`silver.quarantine`: rows that failed validation, with `dq_reasons` (comma-separated):
`missing_id`, `missing_eva`, `missing_ride_id`, `missing_planned_time`, `delay_out_of_range`
(< −30 or > 1 440 min), `departure_before_arrival`, `date_outside_source_month` (more than one day outside the file's month).

`silver._run_metrics`: per month: bronze rows, quarantined, outside-month rows (stops dated in the neighbouring month, which belong to that month's file), duplicates removed, silver rows, delay agreement rate.

## Gold – dimensions and fact

| Table | Grain | Key columns |
|---|---|---|
| `dim_station` | station | `eva`, `station_name` (unique display name, see `sql/gold/01_dim_station.sql`), `base_name` (DB's general name, may be shared by S-Bahn/lower-level platforms), `first_seen_date`, `last_seen_date`, `stop_events`, `journeys`, `train_types` |
| `dim_date` | calendar day | `date`, `weekday`, `weekday_name`, `iso_week`, `is_weekend`, `is_public_holiday` (nationwide), `holiday_name`, `states_with_holiday` (0–16) |
| `dim_train_type` | train type | `train_type`, `train_category`, `stop_events` |
| `fct_stop_events` | one scheduled stop of one ride | silver columns plus `train_category`, `is_arrival_measured` (has a planned arrival and was not cancelled), `is_arrival_punctual` (measured and < 6 min late) |

## Gold – aggregates

| Table | Grain | Main measures |
|---|---|---|
| `agg_punctuality_daily` | day × category (+ `all_rail`) | scheduled stops, measured/punctual arrivals, `punctuality_pct`, `cancellation_pct`, `avg_arrival_delay_min`, `p90_arrival_delay_min`, `punctuality_pct_7d_avg`, holiday flags |
| `agg_hourly_profile` | category × weekday × hour | `punctuality_pct`, `avg_arrival_delay_min`, `cancellation_pct` |
| `agg_station_monthly` | month × station | punctuality, cancellations, `late_15min_pct`, `is_rankable`, `worst_punctuality_rank`, `punctuality_change_pp` vs. previous month |
| `agg_segment_hotspots` | from-station → to-station | `observations`, `median_planned_min`, `median_gain_per_hour` (ranking metric), `median_delay_gain_min`, `pct_trains_gaining_3min`, `p90_delay_gain_min`, `avg_delay_gain_min`, `hotspot_rank` |
| `agg_line_reliability` | line (regional) or train (long-distance) | rides, stations served, punctuality, cancellations, `worst_rank_in_category` |
| `agg_official_comparison` | month × segment (`fernverkehr`, `regio`, `gesamt`) | `our_punctuality_pct`, `official_punctuality_pct`, `difference_pp` |
| `agg_delay_distribution` | category × delay bucket | `arrivals`, `pct_of_category` |
| `dq_results` | check × run | `check`, `layer`, `severity` (ERROR/WARN), `status`, `observed`, `threshold` |

## Gold – machine learning

`ml_features`: one row per measured arrival, with label `label_late` = arrival ≥ 6 min late.

| Feature | Meaning (all computed from days **before** the service day) |
|---|---|
| `station_late_rate_7d`, `station_avg_delay_7d`, `station_volume_7d` | the station's last 7 days |
| `station_hour_late_rate_hist` | the station's late rate at this hour, over all earlier days |
| `route_late_rate_7d` | the line's / train's last 7 days |
| `network_late_rate_prev_day` | whole network, previous day |
| `event_hour`, `weekday`, `is_weekend`, `is_public_holiday`, `states_with_holiday` | calendar (known in advance) |
| `train_category`, `train_type`, `stop_role`, `stop_seq` | timetable (known in advance) |

`ml_training_set`: a reproducible random sample of `ml_features` (at most `ml.max_rows` rows) used by scikit-learn.
