-- fct_stop_events: grain = one scheduled stop of one train ride at one station.
--
-- Punctuality follows Deutsche Bahn's operational definition:
--   * measured at ARRIVAL (origin stations have no arrival and are not measured)
--   * a stop is punctual when the train arrives less than ${punctual_threshold_min} minutes late
--   * cancelled stops are EXCLUDED from the punctuality rate (reported separately)
SELECT
    s.id                                   AS stop_event_id,
    s.source_month,
    s.service_date,
    s.event_hour,
    s.weekday,
    s.eva,
    s.train_type,
    COALESCE(t.train_category, 'other')    AS train_category,
    s.train_number,
    s.line_number,
    s.train_label,
    s.ride_id,
    s.stop_seq,
    s.stop_role,
    s.arrival_planned_ts,
    s.departure_planned_ts,
    s.arrival_delay_min,
    s.departure_delay_min,
    s.is_canceled,
    COALESCE(s.arrival_is_canceled, FALSE)   AS arrival_is_canceled,
    COALESCE(s.departure_is_canceled, FALSE) AS departure_is_canceled,
    (s.arrival_planned_ts IS NOT NULL
        AND NOT COALESCE(s.arrival_is_canceled, FALSE))            AS is_arrival_measured,
    CASE
        WHEN s.arrival_planned_ts IS NOT NULL AND NOT COALESCE(s.arrival_is_canceled, FALSE)
        THEN s.arrival_delay_min < ${punctual_threshold_min}
    END                                    AS is_arrival_punctual,
    s.prev_eva,
    s.segment_planned_min,
    s.delay_gain_min,
    COALESCE(s.is_replacement_train, FALSE) AS is_replacement_train,
    COALESCE(s.is_additional_stop, FALSE)   AS is_additional_stop
FROM silver_stop_events AS s
LEFT JOIN gold_dim_train_type AS t
    ON s.train_type = t.train_type
