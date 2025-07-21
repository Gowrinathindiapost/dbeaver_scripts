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
),

overlaps AS (
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
FROM overlaps
GROUP BY license_number, month
ORDER BY license_number, month;
