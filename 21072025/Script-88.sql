select * from carriermgmt.schedule_vehicle_driver_mapping s where s.driving_license_number_prim ='TA0920224567811842' 
153	GEN	21611001	21090736	2025-07-09	17:10:00	1898	CCRC Bengaluru_Sugganahalli S.O_05:01_MMS Schedule	KM01KK0101	Delivery	2220.0	COMPLETE	2220.0	0.0	2025-07-10	17:11:47	ROOPA A	ROOPA A	NA		0001-01-01 00:00:00.000	false	testing Doeee	TA0920224567811842	1234567891			0	UNMAPPED	2025-07-09 17:11:05.246	ROOPA A	ROOPA A	2025-07-09 00:00:00.000	21610000	21610000	CCRC Bengaluru	Sugganahalli S.O
INSERT INTO carriermgmt.schedule_vehicle_driver_mapping (mapping_id, trip_type, source_office_id, destination_office_id, trip_start_date, trip_start_time, schedule_id, schedule_name, vehicle_reg_number, purpose_for_trip, trip_start_reading, trip_status, trip_end_reading, distance_covered, trip_end_date, trip_end_time, trip_start_by, trip_end_by, reason_for_trip_cancel, trip_cancel_by, trip_cancel_date, long_distance, driver_name_prim, driving_license_number_prim, driver_mobile_number_prim, driver_name_sec, driving_license_number_sec, driver_mobile_number_sec, mapping_status, created_date, created_by, updated_by, updated_date, trip_start_by_office_id, trip_end_by_office_id, source_facility_name, destination_facility_name) VALUES(nextval('carriermgmt.schedule_vehicle_driver_mapping_mapping_id_seq'::regclass), '', 0, 0, '', '', 0, '', '', '', 0, '', 0, 0, '', '', '', '', '', '', '', false, '', '', 0, '', '', 0, '', now(), '', '', '', 0, 0, '', '');

INSERT INTO carriermgmt.schedule_vehicle_driver_mapping (
    --mapping_id, 
    trip_type, source_office_id, destination_office_id, 
    trip_start_date, trip_start_time, schedule_id, schedule_name, 
    vehicle_reg_number, purpose_for_trip, trip_start_reading, trip_status, 
    trip_end_reading, distance_covered, trip_end_date, trip_end_time, 
    trip_start_by, trip_end_by, reason_for_trip_cancel, trip_cancel_by, trip_cancel_date, 
    long_distance, driver_name_prim, driving_license_number_prim, driver_mobile_number_prim, 
    driver_name_sec, driving_license_number_sec, driver_mobile_number_sec, mapping_status, 
    created_date, created_by, updated_by, updated_date, trip_start_by_office_id, trip_end_by_office_id, 
    source_facility_name, destination_facility_name
) VALUES (
   -- 153, 
    'GEN', 
    21611001, 
    21090736, 
    '2025-07-01', 
    '17:10:00', 
    1898, 
    'CCRC Bengaluru_Sugganahalli S.O_05:01_MMS Schedule', 
    'KM01KK0101', 
    'Delivery', 
    2220.0, 
    'COMPLETE', 
    2220.0, 
    0.0, 
    '2025-07-10', 
    '17:11:47', 
    'ROOPA A', 
    'ROOPA A', 
    'NA', 
    NULL, 
    '0001-01-01 00:00:00', 
    false, 
    'testing Doeee', 
    'TA0920224567811842', 
    1234567891, 
    NULL, 
    NULL, 
    NULL, 
    'UNMAPPED', 
    '2025-07-09 17:11:05.246', 
    'ROOPA A', 
    'ROOPA A', 
    '2025-07-09 00:00:00', 
    21610000, 
    21610000, 
    'CCRC Bengaluru', 
    'Sugganahalli S.O'
);



SELECT
    driving_license_number_prim AS license_number,
    DATE_TRUNC('month', make_timestamp(EXTRACT(YEAR FROM trip_start_date)::int, 
                                       EXTRACT(MONTH FROM trip_start_date)::int, 
                                       EXTRACT(DAY FROM trip_start_date)::int, 
                                       EXTRACT(HOUR FROM trip_start_time)::int, 
                                       EXTRACT(MINUTE FROM trip_start_time)::int, 
                                       EXTRACT(SECOND FROM trip_start_time)::double precision)) AS month,
    SUM(
        EXTRACT(
            epoch FROM
            (
                make_timestamp(EXTRACT(YEAR FROM trip_end_date)::int, 
                               EXTRACT(MONTH FROM trip_end_date)::int, 
                               EXTRACT(DAY FROM trip_end_date)::int, 
                               EXTRACT(HOUR FROM trip_end_time)::int, 
                               EXTRACT(MINUTE FROM trip_end_time)::int, 
                               EXTRACT(SECOND FROM trip_end_time)::double precision)
                -
                make_timestamp(EXTRACT(YEAR FROM trip_start_date)::int, 
                               EXTRACT(MONTH FROM trip_start_date)::int, 
                               EXTRACT(DAY FROM trip_start_date)::int, 
                               EXTRACT(HOUR FROM trip_start_time)::int, 
                               EXTRACT(MINUTE FROM trip_start_time)::int, 
                               EXTRACT(SECOND FROM trip_start_time)::double precision)
            )
        ) / 3600  -- convert seconds to hours
    ) AS total_working_hours
FROM carriermgmt.schedule_vehicle_driver_mapping
WHERE trip_end_date IS NOT NULL  -- ensure trip has ended
  AND trip_start_date >= '2025-07-01'
  AND trip_start_date < '2025-08-01'
GROUP BY driving_license_number_prim, month
ORDER BY license_number;




SELECT license_number, month, SUM(total_working_hours) AS total_working_hours
FROM (
    -- primary driver
    SELECT
        driving_license_number_prim AS license_number,
        DATE_TRUNC('month', make_timestamp(EXTRACT(YEAR FROM trip_start_date)::int, 
                                           EXTRACT(MONTH FROM trip_start_date)::int, 
                                           EXTRACT(DAY FROM trip_start_date)::int, 
                                           EXTRACT(HOUR FROM trip_start_time)::int, 
                                           EXTRACT(MINUTE FROM trip_start_time)::int, 
                                           EXTRACT(SECOND FROM trip_start_time)::double precision)) AS month,
        EXTRACT(
            epoch FROM
            (
                make_timestamp(EXTRACT(YEAR FROM trip_end_date)::int, 
                               EXTRACT(MONTH FROM trip_end_date)::int, 
                               EXTRACT(DAY FROM trip_end_date)::int, 
                               EXTRACT(HOUR FROM trip_end_time)::int, 
                               EXTRACT(MINUTE FROM trip_end_time)::int, 
                               EXTRACT(SECOND FROM trip_end_time)::double precision)
                -
                make_timestamp(EXTRACT(YEAR FROM trip_start_date)::int, 
                               EXTRACT(MONTH FROM trip_start_date)::int, 
                               EXTRACT(DAY FROM trip_start_date)::int, 
                               EXTRACT(HOUR FROM trip_start_time)::int, 
                               EXTRACT(MINUTE FROM trip_start_time)::int, 
                               EXTRACT(SECOND FROM trip_start_time)::double precision)
            )
        ) / 3600 AS total_working_hours
    FROM carriermgmt.schedule_vehicle_driver_mapping
    WHERE trip_end_date IS NOT NULL
      AND trip_start_date >= '2025-07-01'
      AND trip_start_date < '2025-08-01'

    UNION ALL

    -- secondary driver
    SELECT
        driving_license_number_sec AS license_number,
        DATE_TRUNC('month', make_timestamp(EXTRACT(YEAR FROM trip_start_date)::int, 
                                           EXTRACT(MONTH FROM trip_start_date)::int, 
                                           EXTRACT(DAY FROM trip_start_date)::int, 
                                           EXTRACT(HOUR FROM trip_start_time)::int, 
                                           EXTRACT(MINUTE FROM trip_start_time)::int, 
                                           EXTRACT(SECOND FROM trip_start_time)::double precision)) AS month,
        EXTRACT(
            epoch FROM
            (
                make_timestamp(EXTRACT(YEAR FROM trip_end_date)::int, 
                               EXTRACT(MONTH FROM trip_end_date)::int, 
                               EXTRACT(DAY FROM trip_end_date)::int, 
                               EXTRACT(HOUR FROM trip_end_time)::int, 
                               EXTRACT(MINUTE FROM trip_end_time)::int, 
                               EXTRACT(SECOND FROM trip_end_time)::double precision)
                -
                make_timestamp(EXTRACT(YEAR FROM trip_start_date)::int, 
                               EXTRACT(MONTH FROM trip_start_date)::int, 
                               EXTRACT(DAY FROM trip_start_date)::int, 
                               EXTRACT(HOUR FROM trip_start_time)::int, 
                               EXTRACT(MINUTE FROM trip_start_time)::int, 
                               EXTRACT(SECOND FROM trip_start_time)::double precision)
            )
        ) / 3600 AS total_working_hours
    FROM carriermgmt.schedule_vehicle_driver_mapping
    WHERE trip_end_date IS NOT NULL
      AND trip_start_date >= '2025-07-01'
      AND trip_start_date < '2025-08-01'
      AND driving_license_number_sec IS NOT NULL
) t
GROUP BY license_number, month
ORDER BY license_number;




WITH trip_times AS (
    SELECT
        mapping_id,
        driving_license_number_prim AS license_number,
        make_timestamp(EXTRACT(YEAR FROM trip_start_date)::int, 
                       EXTRACT(MONTH FROM trip_start_date)::int, 
                       EXTRACT(DAY FROM trip_start_date)::int, 
                       EXTRACT(HOUR FROM trip_start_time)::int, 
                       EXTRACT(MINUTE FROM trip_start_time)::int, 
                       EXTRACT(SECOND FROM trip_start_time)::double precision) AS trip_start_ts,
        make_timestamp(EXTRACT(YEAR FROM trip_end_date)::int, 
                       EXTRACT(MONTH FROM trip_end_date)::int, 
                       EXTRACT(DAY FROM trip_end_date)::int, 
                       EXTRACT(HOUR FROM trip_end_time)::int, 
                       EXTRACT(MINUTE FROM trip_end_time)::int, 
                       EXTRACT(SECOND FROM trip_end_time)::double precision) AS trip_end_ts
    FROM carriermgmt.schedule_vehicle_driver_mapping
    WHERE trip_end_date IS NOT NULL
      AND trip_start_date >= '2025-07-01'
      AND trip_start_date < '2025-08-01'
)

, trip_durations AS (
    SELECT
        license_number,
        DATE_TRUNC('month', trip_start_ts) AS month,
        EXTRACT(epoch FROM (trip_end_ts - trip_start_ts)) / 3600 AS total_working_hours,

        -- Approximate night hours: number of hours where trip_start_ts or trip_end_ts falls between 22:00–23:59 or 00:00–05:59
        CASE
            WHEN (EXTRACT(HOUR FROM trip_start_ts) >= 22 OR EXTRACT(HOUR FROM trip_start_ts) < 6)
              OR (EXTRACT(HOUR FROM trip_end_ts) >= 22 OR EXTRACT(HOUR FROM trip_end_ts) < 6)
            THEN LEAST(EXTRACT(epoch FROM (trip_end_ts - trip_start_ts)) / 3600, 8) -- assume max 8h night
            ELSE 0
        END AS night_hours
    FROM trip_times
)

SELECT
    license_number,
    month,
    SUM(total_working_hours) AS total_working_hours,
    SUM(night_hours) AS total_night_hours,
    SUM(night_hours) * (10.0/60.0) AS nda_hours,
    SUM(total_working_hours) + SUM(night_hours) * (10.0/60.0) AS total_with_nda
FROM trip_durations
GROUP BY license_number, month
ORDER BY license_number;



WITH trip_times AS (
    SELECT
        mapping_id,
        driving_license_number_prim AS license_number,
        make_timestamp(EXTRACT(YEAR FROM trip_start_date)::int, 
                       EXTRACT(MONTH FROM trip_start_date)::int, 
                       EXTRACT(DAY FROM trip_start_date)::int, 
                       EXTRACT(HOUR FROM trip_start_time)::int, 
                       EXTRACT(MINUTE FROM trip_start_time)::int, 
                       EXTRACT(SECOND FROM trip_start_time)::double precision) AS trip_start_ts,
        make_timestamp(EXTRACT(YEAR FROM trip_end_date)::int, 
                       EXTRACT(MONTH FROM trip_end_date)::int, 
                       EXTRACT(DAY FROM trip_end_date)::int, 
                       EXTRACT(HOUR FROM trip_end_time)::int, 
                       EXTRACT(MINUTE FROM trip_end_time)::int, 
                       EXTRACT(SECOND FROM trip_end_time)::double precision) AS trip_end_ts
    FROM carriermgmt.schedule_vehicle_driver_mapping
    WHERE trip_end_date IS NOT NULL
      AND trip_start_date >= '2025-07-01'
      AND trip_start_date < '2025-08-01'
)

, night_overlap AS (
    SELECT
        license_number,
        trip_start_ts,
        trip_end_ts,
        DATE_TRUNC('month', trip_start_ts) AS month,

        -- build night window from trip_start_date 22:00 → trip_start_date+1 06:00
        make_timestamp(EXTRACT(YEAR FROM trip_start_ts)::int, 
                       EXTRACT(MONTH FROM trip_start_ts)::int, 
                       EXTRACT(DAY FROM trip_start_ts)::int, 
                       22,0,0) AS night_start,

        make_timestamp(EXTRACT(YEAR FROM trip_start_ts + interval '1 day')::int, 
                       EXTRACT(MONTH FROM trip_start_ts + interval '1 day')::int, 
                       EXTRACT(DAY FROM trip_start_ts + interval '1 day')::int, 
                       6,0,0) AS night_end
    FROM trip_times
)

, trip_night_calc AS (
    SELECT
        license_number,
        month,
        EXTRACT(epoch FROM (trip_end_ts - trip_start_ts)) / 3600 AS total_working_hours,

        -- compute overlap: GREATEST(start), LEAST(end)
        GREATEST(LEAST(trip_end_ts, night_end) - GREATEST(trip_start_ts, night_start), interval '0') AS night_interval
    FROM night_overlap
)

SELECT
    license_number,
    month,
    SUM(total_working_hours) AS total_working_hours,
    SUM(EXTRACT(epoch FROM night_interval) / 3600) AS total_night_hours,
    SUM(EXTRACT(epoch FROM night_interval) / 3600) * (10.0/60.0) AS nda_hours,
    SUM(total_working_hours) + SUM(EXTRACT(epoch FROM night_interval) / 3600) * (10.0/60.0) AS total_with_nda
FROM trip_night_calc
GROUP BY license_number, month
ORDER BY license_number;




--for multiple nights
WITH trip_times AS (
    SELECT
        mapping_id,
        driving_license_number_prim AS license_number,
        make_timestamp(EXTRACT(YEAR FROM trip_start_date)::int, 
                       EXTRACT(MONTH FROM trip_start_date)::int, 
                       EXTRACT(DAY FROM trip_start_date)::int, 
                       EXTRACT(HOUR FROM trip_start_time)::int, 
                       EXTRACT(MINUTE FROM trip_start_time)::int, 
                       EXTRACT(SECOND FROM trip_start_time)::double precision) AS trip_start_ts,
        make_timestamp(EXTRACT(YEAR FROM trip_end_date)::int, 
                       EXTRACT(MONTH FROM trip_end_date)::int, 
                       EXTRACT(DAY FROM trip_end_date)::int, 
                       EXTRACT(HOUR FROM trip_end_time)::int, 
                       EXTRACT(MINUTE FROM trip_end_time)::int, 
                       EXTRACT(SECOND FROM trip_end_time)::double precision) AS trip_end_ts
    FROM carriermgmt.schedule_vehicle_driver_mapping
    WHERE trip_end_date IS NOT NULL
      AND trip_start_date >= '2025-07-01'
      AND trip_start_date < '2025-08-01'
)

, night_windows AS (
    SELECT
        tt.license_number,
        DATE_TRUNC('month', tt.trip_start_ts) AS month,
        tt.trip_start_ts,
        tt.trip_end_ts,
        generate_series(
            date_trunc('day', tt.trip_start_ts),
            date_trunc('day', tt.trip_end_ts),
            interval '1 day'
        ) AS night_date
    FROM trip_times tt
)

, night_periods AS (
    SELECT
        license_number,
        month,
        trip_start_ts,
        trip_end_ts,
        make_timestamp(EXTRACT(YEAR FROM night_date)::int, 
                       EXTRACT(MONTH FROM night_date)::int, 
                       EXTRACT(DAY FROM night_date)::int, 
                       22,0,0) AS night_start,
        make_timestamp(EXTRACT(YEAR FROM night_date + interval '1 day')::int, 
                       EXTRACT(MONTH FROM night_date + interval '1 day')::int, 
                       EXTRACT(DAY FROM night_date + interval '1 day')::int, 
                       6,0,0) AS night_end
    FROM night_windows
)

, overlaps AS (
    SELECT
        license_number,
        month,
        trip_start_ts,
        trip_end_ts,
        GREATEST(LEAST(trip_end_ts, night_end) - GREATEST(trip_start_ts, night_start), interval '0') AS night_overlap
    FROM night_periods
)

, trip_summary AS (
    SELECT
        license_number,
        month,
        EXTRACT(epoch FROM (trip_end_ts - trip_start_ts))/3600 AS total_working_hours,
        SUM(EXTRACT(epoch FROM night_overlap)/3600) AS total_night_hours
    FROM overlaps
    GROUP BY license_number, month, trip_start_ts, trip_end_ts
)

SELECT
    license_number,
    month,
    SUM(total_working_hours) AS total_working_hours,
    SUM(total_night_hours) AS total_night_hours,
    SUM(total_night_hours) * (10.0/60.0) AS nda_hours,
    SUM(total_working_hours) + SUM(total_night_hours) * (10.0/60.0) AS total_with_nda
FROM trip_summary
GROUP BY license_number, month
ORDER BY license_number;





---above not working but below working
--WITH trip_times AS (
--    SELECT
--        mapping_id,
--        driving_license_number_prim AS license_number,
--        make_timestamp(
--            EXTRACT(YEAR FROM trip_start_date)::int, 
--            EXTRACT(MONTH FROM trip_start_date)::int, 
--            EXTRACT(DAY FROM trip_start_date)::int, 
--            EXTRACT(HOUR FROM trip_start_time)::int, 
--            EXTRACT(MINUTE FROM trip_start_time)::int, 
--            EXTRACT(SECOND FROM trip_start_time)::double precision
--        ) AS trip_start_ts,
--        make_timestamp(
--            EXTRACT(YEAR FROM trip_end_date)::int, 
--            EXTRACT(MONTH FROM trip_end_date)::int, 
--            EXTRACT(DAY FROM trip_end_date)::int, 
--            EXTRACT(HOUR FROM trip_end_time)::int, 
--            EXTRACT(MINUTE FROM trip_end_time)::int, 
--            EXTRACT(SECOND FROM trip_end_time)::double precision
--        ) AS trip_end_ts
--    FROM carriermgmt.schedule_vehicle_driver_mapping
--    WHERE trip_end_date IS NOT NULL
--      AND trip_start_date >= '2025-07-01'
--      AND trip_start_date < '2025-08-01'
--)
--
--, night_windows AS (
--    SELECT
--        license_number,
--        trip_start_ts,
--        trip_end_ts,
--        DATE_TRUNC('month', trip_start_ts) AS month,
--        generate_series(
--            date_trunc('day', trip_start_ts),
--            date_trunc('day', trip_end_ts),
--            interval '1 day'
--        ) AS night_date
--    FROM trip_times
--)
--
--, night_periods AS (
--    SELECT
--        license_number,
--        month,
--        trip_start_ts,
--        trip_end_ts,
--        make_timestamp(
--            EXTRACT(YEAR FROM night_date)::int, 
--            EXTRACT(MONTH FROM night_date)::int, 
--            EXTRACT(DAY FROM night_date)::int, 
--            22,0,0
--        ) AS night_start,
--        make_timestamp(
--            EXTRACT(YEAR FROM night_date + interval '1 day')::int, 
--            EXTRACT(MONTH FROM night_date + interval '1 day')::int, 
--            EXTRACT(DAY FROM night_date + interval '1 day')::int, 
--            6,0,0
--        ) AS night_end
--    FROM night_windows
--)
--
--, overlaps AS (
--    SELECT
--        license_number,
--        month,
--        trip_start_ts,
--        trip_end_ts,
--        GREATEST(
--            LEAST(trip_end_ts, night_end) - GREATEST(trip_start_ts, night_start),
--            interval '0'
--        ) AS night_overlap
--    FROM night_periods
--)
--
--, trip_summary AS (
--    SELECT
--        license_number,
--        month,
--        EXTRACT(epoch FROM (trip_end_ts - trip_start_ts))/3600 AS total_working_hours,
--        SUM(EXTRACT(epoch FROM night_overlap)/3600) AS total_night_hours
--    FROM overlaps
--    GROUP BY license_number, month, trip_start_ts, trip_end_ts
--)
--
--SELECT
--    license_number,
--    month,
--    SUM(total_working_hours) AS total_working_hours,
--    SUM(total_night_hours) AS total_night_hours,
--    SUM(total_night_hours) * (10.0/60.0) AS nda_hours,
--    SUM(total_working_hours) + SUM(total_night_hours) * (10.0/60.0) AS total_with_nda
--FROM trip_summary
--GROUP BY license_number, month
--ORDER BY license_number;

WITH trip_times AS (
    SELECT
        mapping_id,
        driving_license_number_prim AS license_number,
        make_timestamp(
            EXTRACT(YEAR FROM trip_start_date)::int, 
            EXTRACT(MONTH FROM trip_start_date)::int, 
            EXTRACT(DAY FROM trip_start_date)::int, 
            EXTRACT(HOUR FROM trip_start_time)::int, 
            EXTRACT(MINUTE FROM trip_start_time)::int, 
            EXTRACT(SECOND FROM trip_start_time)::double precision
        ) AS trip_start_ts,
        make_timestamp(
            EXTRACT(YEAR FROM trip_end_date)::int, 
            EXTRACT(MONTH FROM trip_end_date)::int, 
            EXTRACT(DAY FROM trip_end_date)::int, 
            EXTRACT(HOUR FROM trip_end_time)::int, 
            EXTRACT(MINUTE FROM trip_end_time)::int, 
            EXTRACT(SECOND FROM trip_end_time)::double precision
        ) AS trip_end_ts
    FROM carriermgmt.schedule_vehicle_driver_mapping
    WHERE trip_end_date IS NOT NULL
      AND trip_start_date >= '2025-07-01'
      AND trip_start_date < '2025-08-01'
)

, night_windows AS (
    SELECT
        license_number,
        trip_start_ts,
        trip_end_ts,
        date_trunc('month', trip_start_ts) AS month,
        generate_series(
            date_trunc('day', trip_start_ts),
            date_trunc('day', trip_end_ts),
            interval '1 day'
        ) AS night_date
    FROM trip_times
)

, night_periods AS (
    SELECT
        license_number,
        month,
        trip_start_ts,
        trip_end_ts,
        make_timestamp(
            EXTRACT(YEAR FROM night_date)::int, 
            EXTRACT(MONTH FROM night_date)::int, 
            EXTRACT(DAY FROM night_date)::int, 
            22,0,0
        ) AS night_start,
        make_timestamp(
            EXTRACT(YEAR FROM night_date + interval '1 day')::int, 
            EXTRACT(MONTH FROM night_date + interval '1 day')::int, 
            EXTRACT(DAY FROM night_date + interval '1 day')::int, 
            6,0,0
        ) AS night_end
    FROM night_windows
)

, overlaps AS (
    SELECT
        license_number,
        month,
        trip_start_ts,
        trip_end_ts,
        GREATEST(
            LEAST(trip_end_ts, night_end) - GREATEST(trip_start_ts, night_start),
            interval '0'
        ) AS night_overlap
    FROM night_periods
)

, trip_summary AS (
    SELECT
        license_number,
        month,
        EXTRACT(epoch FROM (trip_end_ts - trip_start_ts)) / 3600 AS total_working_hours,
        SUM(EXTRACT(epoch FROM night_overlap) / 3600) AS total_night_hours
    FROM overlaps
    GROUP BY license_number, month, trip_start_ts, trip_end_ts
)

SELECT
    license_number,
    month,
    SUM(total_working_hours) AS total_working_hours,
    SUM(total_night_hours) AS total_night_hours,
    SUM(total_night_hours) * (10.0 / 60.0) AS nda_hours,
    SUM(total_working_hours) + SUM(total_night_hours) * (10.0 / 60.0) AS total_with_nda
FROM trip_summary
GROUP BY license_number, month
ORDER BY license_number;

WITH trip_times AS (
    SELECT
        mapping_id,
        driving_license_number_prim AS license_number,
        make_timestamp(
            EXTRACT(YEAR FROM trip_start_date)::int, 
            EXTRACT(MONTH FROM trip_start_date)::int, 
            EXTRACT(DAY FROM trip_start_date)::int, 
            EXTRACT(HOUR FROM trip_start_time)::int, 
            EXTRACT(MINUTE FROM trip_start_time)::int, 
            EXTRACT(SECOND FROM trip_start_time)::double precision
        ) AS trip_start_ts,
        make_timestamp(
            EXTRACT(YEAR FROM trip_end_date)::int, 
            EXTRACT(MONTH FROM trip_end_date)::int, 
            EXTRACT(DAY FROM trip_end_date)::int, 
            EXTRACT(HOUR FROM trip_end_time)::int, 
            EXTRACT(MINUTE FROM trip_end_time)::int, 
            EXTRACT(SECOND FROM trip_end_time)::double precision
        ) AS trip_end_ts
    FROM carriermgmt.schedule_vehicle_driver_mapping
    WHERE trip_end_date IS NOT NULL
      AND trip_start_date >= '2025-07-01'
      AND trip_start_date < '2025-08-01'
)

, night_periods AS (
    SELECT
        license_number,
        trip_start_ts,
        trip_end_ts,
        date_trunc('month', trip_start_ts) AS month,
        -- Generate series of night periods for each day the trip spans
        generate_series(
            GREATEST(date_trunc('day', trip_start_ts), date_trunc('day', trip_start_ts + interval '22 hours')), -- Start of the first potential night shift
            LEAST(date_trunc('day', trip_end_ts) + interval '1 day', date_trunc('day', trip_end_ts) + interval '6 hours' + interval '1 day'), -- End of the last potential night shift
            interval '1 day'
        ) AS current_day_start
    FROM trip_times
)

, night_overlaps_per_day AS (
    SELECT
        license_number,
        month,
        trip_start_ts,
        trip_end_ts,
        -- Define the start and end of the night window for the current day
        GREATEST(trip_start_ts, current_day_start + interval '22 hours') AS actual_night_window_start,
        LEAST(trip_end_ts, current_day_start + interval '1 day' + interval '6 hours') AS actual_night_window_end
    FROM night_periods
)

, calculated_overlaps AS (
    SELECT
        license_number,
        month,
        trip_start_ts,
        trip_end_ts,
        -- Calculate the overlap for each potential night window
        EXTRACT(epoch FROM (GREATEST(interval '0', actual_night_window_end - actual_night_window_start))) / 3600 AS night_overlap_hours
    FROM night_overlaps_per_day
    WHERE actual_night_window_end > actual_night_window_start -- Ensure there's a valid overlap
)

SELECT
    license_number,
    month,
    SUM(EXTRACT(epoch FROM (trip_end_ts - trip_start_ts))) / 3600 AS total_working_hours,
    SUM(night_overlap_hours) AS total_night_hours,
    SUM(night_overlap_hours) * (10.0 / 60.0) AS nda_hours,
    (SUM(EXTRACT(epoch FROM (trip_end_ts - trip_start_ts))) / 3600) + (SUM(night_overlap_hours) * (10.0 / 60.0)) AS total_with_nda
FROM calculated_overlaps
GROUP BY license_number, month
ORDER BY license_number, month;





WITH trip_times AS (
    SELECT
        mapping_id,
        driving_license_number_prim AS license_number,
        -- Create timestamp from separate date and time columns for trip start
        make_timestamp(
            EXTRACT(YEAR FROM trip_start_date)::int,
            EXTRACT(MONTH FROM trip_start_date)::int,
            EXTRACT(DAY FROM trip_start_date)::int,
            EXTRACT(HOUR FROM trip_start_time)::int,
            EXTRACT(MINUTE FROM trip_start_time)::int,
            EXTRACT(SECOND FROM trip_start_time)::double precision
        ) AS trip_start_ts,
        -- Create timestamp from separate date and time columns for trip end
        make_timestamp(
            EXTRACT(YEAR FROM trip_end_date)::int,
            EXTRACT(MONTH FROM trip_end_date)::int,
            EXTRACT(DAY FROM trip_end_date)::int,
            EXTRACT(HOUR FROM trip_end_time)::int,
            EXTRACT(MINUTE FROM trip_end_time)::int,
            EXTRACT(SECOND FROM trip_end_time)::double precision
        ) AS trip_end_ts
    FROM carriermgmt.schedule_vehicle_driver_mapping
    WHERE trip_end_date IS NOT NULL
      AND trip_start_date >= '2025-07-01'
      AND trip_start_date < '2025-08-01'
),
-- This CTE generates a series of dates for each trip, covering every day the trip spans.
-- This is crucial for correctly identifying night overlaps across midnight.
night_windows AS (
    SELECT
        license_number,
        trip_start_ts,
        trip_end_ts,
        date_trunc('month', trip_start_ts) AS month,
        generate_series(
            date_trunc('day', trip_start_ts),
            date_trunc('day', trip_end_ts),
            interval '1 day'
        ) AS night_date -- Represents each day the trip is active
    FROM trip_times
),
-- This CTE defines the fixed night period (10 PM to 6 AM) for each relevant date.
night_periods AS (
    SELECT
        license_number,
        month,
        trip_start_ts,
        trip_end_ts,
        -- Night starts at 10 PM on night_date
        make_timestamp(
            EXTRACT(YEAR FROM night_date)::int,
            EXTRACT(MONTH FROM night_date)::int,
            EXTRACT(DAY FROM night_date)::int,
            22,0,0
        ) AS night_start,
        -- Night ends at 6 AM on the *next* day
        make_timestamp(
            EXTRACT(YEAR FROM night_date + interval '1 day')::int,
            EXTRACT(MONTH FROM night_date + interval '1 day')::int,
            EXTRACT(DAY FROM night_date + interval '1 day')::int,
            6,0,0
        ) AS night_end
    FROM night_windows
),
-- This CTE calculates the actual overlap between each trip segment and the defined night periods.
overlaps AS (
    SELECT
        license_number,
        month,
        trip_start_ts,
        trip_end_ts,
        -- Calculate the duration of the overlap. GREATEST ensures non-negative duration.
        GREATEST(
            LEAST(trip_end_ts, night_end) - GREATEST(trip_start_ts, night_start),
            interval '0'
        ) AS night_overlap
    FROM night_periods
)
-- The final SELECT statement aggregates the total working hours, total night hours,
-- and calculates NDA hours and total hours with NDA for each driver per month.
SELECT
    license_number,
    month,
    -- Sum of all trip durations for the driver within the month
    SUM(EXTRACT(epoch FROM (trip_end_ts - trip_start_ts))) / 3600 AS total_working_hours,
    -- Sum of all night overlaps for the driver within the month
    SUM(EXTRACT(epoch FROM night_overlap) / 3600) AS total_night_hours,
    -- Calculate NDA hours (total night hours * 10 minutes/hour)
    SUM(EXTRACT(epoch FROM night_overlap) / 3600) * (10.0 / 60.0) AS nda_hours,
    -- Total hours including NDA: sum of working hours + NDA hours
    (SUM(EXTRACT(epoch FROM (trip_end_ts - trip_start_ts))) / 3600) + (SUM(EXTRACT(epoch FROM night_overlap) / 3600) * (10.0 / 60.0)) AS total_with_nda
FROM overlaps
GROUP BY license_number, month
ORDER BY license_number, month;



WITH trip_times AS (
    SELECT
        mapping_id,
        driving_license_number_prim AS license_number,
        make_timestamp(
            EXTRACT(YEAR FROM trip_start_date)::int,
            EXTRACT(MONTH FROM trip_start_date)::int,
            EXTRACT(DAY FROM trip_start_date)::int,
            EXTRACT(HOUR FROM trip_start_time)::int,
            EXTRACT(MINUTE FROM trip_start_time)::int,
            EXTRACT(SECOND FROM trip_start_time)::double precision
        ) AS trip_start_ts,
        make_timestamp(
            EXTRACT(YEAR FROM trip_end_date)::int,
            EXTRACT(MONTH FROM trip_end_date)::int,
            EXTRACT(DAY FROM trip_end_date)::int,
            EXTRACT(HOUR FROM trip_end_time)::int,
            EXTRACT(MINUTE FROM trip_end_time)::int,
            EXTRACT(SECOND FROM trip_end_time)::double precision
        ) AS trip_end_ts
    FROM carriermgmt.schedule_vehicle_driver_mapping
    WHERE trip_end_date IS NOT NULL
      AND trip_start_date >= '2025-07-01'
      AND trip_start_date < '2025-08-01'
),

night_windows AS (
    SELECT
        license_number,
        trip_start_ts,
        trip_end_ts,
        date_trunc('month', trip_start_ts) AS month,
        generate_series(
            date_trunc('day', trip_start_ts),
            date_trunc('day', trip_end_ts),
            interval '1 day'
        ) AS night_date
    FROM trip_times
),

night_periods AS (
    SELECT
        license_number,
        month,
        trip_start_ts,
        trip_end_ts,
        make_timestamp(
            EXTRACT(YEAR FROM  )::int,
            EXTRACT(MONTH FROM night_date)::int,
            EXTRACT(DAY FROM night_date)::int,
            22,0,0
        ) AS night_start,
        make_timestamp(
            EXTRACT(YEAR FROM night_date + interval '1 day')::int,
            EXTRACT(MONTH FROM night_date + interval '1 day')::int,
            EXTRACT(DAY FROM night_date + interval '1 day')::int,
            6,0,0
        ) AS night_end
    FROM night_windows
),

dutyHours AS (
    SELECT
        license_number,
        month,
        trip_start_ts,
        trip_end_ts,
        GREATEST(
            LEAST(trip_end_ts, night_end) - GREATEST(trip_start_ts, night_start),
            interval '0'
        ) AS night_overlap
    FROM night_periods
)

SELECT
    license_number,
    month,
    SUM(EXTRACT(epoch FROM (trip_end_ts - trip_start_ts)) / 3600) AS total_working_hours,
    SUM(EXTRACT(epoch FROM night_overlap) / 3600) AS total_night_hours,
    SUM(EXTRACT(epoch FROM night_overlap) / 3600) * (10.0 / 60.0) AS nda_hours,
    SUM(EXTRACT(epoch FROM (trip_end_ts - trip_start_ts)) / 3600) + 
    SUM(EXTRACT(epoch FROM night_overlap) / 3600) * (10.0 / 60.0) AS total_with_nda
FROM dutyHours
GROUP BY license_number, month
ORDER BY license_number, month;

