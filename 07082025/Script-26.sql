select *  from carriermgmt.schedule_vehicle_driver_mapping svdm where svdm.driving_license_number_prim ='DE098765431' 'TA092022456781234544'

select mapping_id,trip_start_date,trip_end_date,trip_start_time ,trip_end_time  from carriermgmt.schedule_vehicle_driver_mapping svdm where svdm.driving_license_number_prim ='TA092022456781234544'
select mapping_id,trip_start_date,trip_end_date,trip_start_time ,trip_end_time  from carriermgmt.schedule_vehicle_driver_mapping svdm where 
svdm.driving_license_number_prim ='DE098765431' or svdm.driving_license_number_sec ='DE098765431' and trip_start_date='29-04-2025 '
SELECT 
    TO_CHAR(
        SUM(trip_end_time - trip_start_time), 
        'HH24:MI:SS'
    ) AS total_time
FROM 
    carriermgmt.schedule_vehicle_driver_mapping svdm 
WHERE 
    svdm.driving_license_number_prim = 'TA092022456781234544' order by mapping_id desc;



SELECT 
    SUM(EXTRACT(EPOCH FROM (trip_end_time - trip_start_time))) / 3600 AS total_hours
FROM 
    carriermgmt.schedule_vehicle_driver_mapping svdm 
WHERE 
    svdm.driving_license_number_prim = 'TA092022456781234544';

;


WITH time_diff AS (
    SELECT 
        trip_end_time - trip_start_time AS duration
    FROM 
        carriermgmt.schedule_vehicle_driver_mapping svdm 
    WHERE 
        svdm.driving_license_number_prim = 'DE098765431' --'TA092022456781234544'
)
SELECT 
    EXTRACT(HOUR FROM sum_duration) || ':' ||
    LPAD(EXTRACT(MINUTE FROM sum_duration)::text, 2, '0') || ':' ||
    LPAD(EXTRACT(SECOND FROM sum_duration)::text, 2, '0') AS total_time
FROM (
    SELECT SUM(duration) AS sum_duration FROM time_diff
) t;



SELECT 
    mapping_id,
    trip_start_date,
    trip_end_date,
    trip_start_time,
    trip_end_time,
    (trip_end_time - trip_start_time) AS duration
FROM 
    carriermgmt.schedule_vehicle_driver_mapping svdm 
WHERE 
    (svdm.driving_license_number_prim = 'DE098765431' 
     OR svdm.driving_license_number_sec = 'DE098765431')
    AND trip_start_date = '2025-04-29'
ORDER BY 
    mapping_id DESC;

-----
WITH trip_data AS (
    SELECT 
        mapping_id,
        trip_start_date,
        trip_end_date,
        trip_start_time,
        trip_end_time,
        (trip_end_time - trip_start_time) AS duration,
        -- Calculate night hours (10PM-6AM)
        GREATEST(
            LEAST(
                CASE WHEN trip_end_time::time >= '22:00:00'::time 
                     THEN trip_end_time 
                     ELSE trip_end_date::timestamp + '22:00:00'::interval END,
                CASE WHEN trip_end_time::time >= '06:00:00'::time 
                     THEN trip_end_date::timestamp + '06:00:00'::interval 
                     ELSE trip_end_time END
            ) - 
            GREATEST(
                CASE WHEN trip_start_time::time >= '22:00:00'::time 
                     THEN trip_start_time 
                     ELSE trip_start_date::timestamp + '22:00:00'::interval END,
                CASE WHEN trip_start_time::time >= '06:00:00'::time 
                     THEN trip_start_date::timestamp + '06:00:00'::interval 
                     ELSE trip_start_time END
            ),
            '00:00:00'::interval
        ) AS night_hours
    FROM 
        carriermgmt.schedule_vehicle_driver_mapping svdm 
    WHERE 
        (svdm.driving_license_number_prim = 'DE098765431' 
         OR svdm.driving_license_number_sec = 'DE098765431')
        AND trip_start_date = '2025-04-29'::date
)
SELECT 
    mapping_id,
    trip_start_date,
    trip_end_date,
    trip_start_time,
    trip_end_time,
    duration,
    night_hours,
    -- Calculate night coefficient (10 minutes per night hour)
    ROUND((EXTRACT(EPOCH FROM night_hours)/3600 * 10, 2) AS night_coefficient_minutes,
    -- Total duration with night coefficient added as time
    duration + (ROUND(EXTRACT(EPOCH FROM night_hours)/3600 * 10) * INTERVAL '1 minute' AS total_duration_with_coefficient
FROM 
    trip_data
ORDER BY 
    mapping_id DESC;

----
WITH trip_data AS (
    SELECT 
        mapping_id,
        trip_start_date,
        trip_end_date,
        trip_start_time,
        trip_end_time,
        (trip_end_time - trip_start_time) AS duration,
        -- Calculate night hours (10PM-6AM)
        GREATEST(
            LEAST(
                CASE WHEN trip_end_time::time >= '22:00:00'::time 
                     THEN trip_end_time 
                     ELSE (trip_end_date::timestamp + '22:00:00'::interval) END,
                CASE WHEN trip_end_time::time >= '06:00:00'::time 
                     THEN (trip_end_date::timestamp + '06:00:00'::interval)
                     ELSE trip_end_time END
            ) - 
            GREATEST(
                CASE WHEN trip_start_time::time >= '22:00:00'::time 
                     THEN trip_start_time 
                     ELSE (trip_start_date::timestamp + '22:00:00'::interval) END,
                CASE WHEN trip_start_time::time >= '06:00:00'::time 
                     THEN (trip_start_date::timestamp + '06:00:00'::interval)
                     ELSE trip_start_time END
            ),
            '00:00:00'::interval
        ) AS night_hours
    FROM 
        carriermgmt.schedule_vehicle_driver_mapping svdm 
    WHERE 
        (svdm.driving_license_number_prim = 'DE098765431' 
         OR svdm.driving_license_number_sec = 'DE098765431')
        AND trip_start_date = '2025-04-29'::date
)
SELECT 
    mapping_id,
    trip_start_date,
    trip_end_date,
    trip_start_time,
    trip_end_time,
    duration,
    night_hours,
    -- Calculate night coefficient (10 minutes per night hour)
    ROUND(EXTRACT(EPOCH FROM night_hours)/3600 * 10, 2) AS night_coefficient_minutes,
    -- Total duration with night coefficient added as time
    (duration + (ROUND(EXTRACT(EPOCH FROM night_hours)/3600 * 10) * INTERVAL '1 minute')) AS total_duration_with_coefficient
FROM 
    trip_data
ORDER BY 
    mapping_id DESC;
--***************
WITH trip_data AS (
    SELECT 
        mapping_id,
        trip_start_date,
        trip_end_date,
        trip_start_time,
        trip_end_time,
        (trip_end_time - trip_start_time) AS duration,
        -- Calculate night hours (10PM-6AM)
        GREATEST(
            LEAST(
                CASE WHEN (trip_end_time::time >= '22:00:00'::time OR trip_end_time::time < '06:00:00'::time)
                     THEN trip_end_time 
                     ELSE (trip_end_date + '22:00:00'::time)::timestamp END,
                CASE WHEN trip_end_time::time >= '06:00:00'::time
                     THEN (trip_end_date + '06:00:00'::time)::timestamp
                     ELSE trip_end_time END
            ) - 
            GREATEST(
                CASE WHEN (trip_start_time::time >= '22:00:00'::time OR trip_start_time::time < '06:00:00'::time)
                     THEN trip_start_time 
                     ELSE (trip_start_date + '22:00:00'::time)::timestamp END,
                CASE WHEN trip_start_time::time >= '06:00:00'::time
                     THEN (trip_start_date + '06:00:00'::time)::timestamp
                     ELSE trip_start_time END
            ),
            '00:00:00'::interval
        ) AS night_hours
    FROM 
        carriermgmt.schedule_vehicle_driver_mapping svdm 
    WHERE 
        (svdm.driving_license_number_prim = 'TA0920224567899' 
         OR svdm.driving_license_number_sec = 'TA0920224567899 ')
        AND trip_start_date = '2024-03-13'::date
)
SELECT 
    mapping_id,
    trip_start_date,
    trip_end_date,
    trip_start_time,
    trip_end_time,
    duration,
    night_hours,
    -- Calculate night coefficient (10 minutes per night hour)
    ROUND(EXTRACT(EPOCH FROM night_hours)/3600 * 10, 2) AS night_coefficient_minutes,
    -- Total duration with night coefficient added as time
   -- (duration + (ROUND(EXTRACT(EPOCH FROM night_hours)/3600 * 10) * INTERVAL '1 minute') AS total_duration_with_coefficient
    (duration + (ROUND(EXTRACT(EPOCH FROM night_hours)/3600 * 10) * INTERVAL '1 minute')) AS total_duration_with_coefficient

FROM 
    trip_data
ORDER BY 
    mapping_id DESC;
-------
WITH trip_data AS (
    SELECT 
        mapping_id,
        trip_start_date,
        trip_end_date,
        trip_start_time,
        trip_end_time,
        (trip_end_time - trip_start_time) AS duration,

        -- Calculate night hours (10PM-6AM)
        (
            -- Hours from midnight to 6AM at trip start
            CASE WHEN trip_start_time::time < '06:00:00'::time 
                 THEN EXTRACT(EPOCH FROM (
                      LEAST(trip_start_date::timestamp + INTERVAL '06:00:00', trip_end_time) - 
                      trip_start_time))
                 ELSE 0 
            END +
            
            -- Hours from 10PM to midnight at trip start
            CASE WHEN trip_start_time::time >= '22:00:00'::time 
                 THEN EXTRACT(EPOCH FROM (
                      LEAST(trip_start_date::timestamp + INTERVAL '1 day', trip_end_time) - 
                      trip_start_time))
                 ELSE 0 
            END +
            
            -- Hours from midnight to 6AM at trip end
            CASE WHEN trip_end_time::time < '06:00:00'::time 
                 THEN EXTRACT(EPOCH FROM (
                      trip_end_time - 
                      GREATEST(trip_end_date::timestamp, trip_start_time)))
                 ELSE 0 
            END +
            
            -- Hours from 10PM to midnight at trip end
            CASE WHEN trip_end_time::time >= '22:00:00'::time 
                 THEN EXTRACT(EPOCH FROM (
                      trip_end_time - 
                      GREATEST(trip_end_date::timestamp + INTERVAL '22:00:00', trip_start_time)))
                 ELSE 0 
            END
        ) / 3600 * INTERVAL '1 hour' AS night_hours

    FROM carriermgmt.schedule_vehicle_driver_mapping svdm 
    WHERE 
        (svdm.driving_license_number_prim = 'TA0920224567899' 
         OR svdm.driving_license_number_sec = 'TA0920224567899')
        AND trip_start_date = '2024-03-13'::date
)

SELECT 
    mapping_id,
    trip_start_date,
    trip_end_date,
    trip_start_time,
    trip_end_time,
    duration,
    GREATEST(night_hours, INTERVAL '0') AS night_hours,
    -- NDA coefficient: 10 minutes per night hour
    ROUND(EXTRACT(EPOCH FROM GREATEST(night_hours, INTERVAL '0')) / 3600 * 10, 2) AS night_coefficient_minutes,
    -- Total duration with NDA time added
    duration + (ROUND(EXTRACT(EPOCH FROM GREATEST(night_hours, INTERVAL '0')) / 3600 * 10) * INTERVAL '1 minute') AS total_duration_with_coefficient
FROM trip_data
ORDER BY mapping_id desc

---
WITH trip_data AS (
    SELECT 
    svdm.schedule_id,
	svdm.schedule_name,
vehicle_reg_number,
        mapping_id,
        trip_start_date,
        trip_end_date,
        (trip_start_date::timestamp + trip_start_time) AS trip_start_ts,
        (trip_end_date::timestamp + trip_end_time) AS trip_end_ts,
        (trip_end_date::timestamp + trip_end_time) - (trip_start_date::timestamp + trip_start_time) AS duration,

        -- Calculate night hours (10PM-6AM)
        (
            -- Hours from midnight to 6AM at trip start
            CASE WHEN (trip_start_date::timestamp + trip_start_time)::time < '06:00:00'::time 
                 THEN EXTRACT(EPOCH FROM (
                      LEAST(trip_start_date::timestamp + INTERVAL '06:00:00', (trip_end_date::timestamp + trip_end_time)) - 
                      (trip_start_date::timestamp + trip_start_time)))
                 ELSE 0 
            END
            +
            -- Hours from 10PM to midnight at trip start
            CASE WHEN (trip_start_date::timestamp + trip_start_time)::time >= '22:00:00'::time 
                 THEN EXTRACT(EPOCH FROM (
                      LEAST(trip_start_date::timestamp + INTERVAL '1 day', (trip_end_date::timestamp + trip_end_time)) - 
                      (trip_start_date::timestamp + trip_start_time)))
                 ELSE 0 
            END
            +
            -- Hours from midnight to 6AM at trip end
            CASE WHEN (trip_end_date::timestamp + trip_end_time)::time < '06:00:00'::time 
                 THEN EXTRACT(EPOCH FROM (
                      (trip_end_date::timestamp + trip_end_time) - 
                      GREATEST(trip_end_date::timestamp, (trip_start_date::timestamp + trip_start_time))))
                 ELSE 0 
            END
            +
            -- Hours from 10PM to midnight at trip end
            CASE WHEN (trip_end_date::timestamp + trip_end_time)::time >= '22:00:00'::time 
                 THEN EXTRACT(EPOCH FROM (
                      (trip_end_date::timestamp + trip_end_time) - 
                      GREATEST(trip_end_date::timestamp + INTERVAL '22:00:00', (trip_start_date::timestamp + trip_start_time))))
                 ELSE 0 
            END
        ) / 3600 * INTERVAL '1 hour' AS night_hours

    FROM carriermgmt.schedule_vehicle_driver_mapping svdm 
    WHERE 
        (svdm.driving_license_number_prim = 'TA0920224567899' 
         OR svdm.driving_license_number_sec = 'TA0920224567899')
        AND trip_start_date = '2024-03-13'::date
)

SELECT 
	schedule_id,
	schedule_name,
vehicle_reg_number,
    mapping_id,    
    trip_start_date,
    trip_end_date,
    trip_start_ts AS trip_start_time,
    trip_end_ts AS trip_end_time,
    duration,
    GREATEST(night_hours, INTERVAL '0') AS night_hours,
    ROUND(EXTRACT(EPOCH FROM GREATEST(night_hours, INTERVAL '0')) / 3600 * 10, 2) AS night_coefficient_minutes,
    duration + (ROUND(EXTRACT(EPOCH FROM GREATEST(night_hours, INTERVAL '0')) / 3600 * 10) * INTERVAL '1 minute') AS total_duration_with_coefficient
FROM trip_data
ORDER BY mapping_id DESC;



UPDATE carriermgmt.schedule_vehicle_driver_mapping
SET
    trip_type = $1,
    destination_office_id = $2,
    destination_facility_name = $3,
    vehicle_reg_number = $4,
    purpose_for_trip = $5,
    trip_start_reading = $6,
    long_distance = $7,
    driver_name_prim = $8,
    driving_license_number_prim = $9,
    driver_mobile_number_prim = $10,
    driver_name_sec = $11,
    driving_license_number_sec = $12,
    driver_mobile_number_sec = $13,
    mapping_status = 'MAPPED',
    updated_by = $14,
    updated_date = $15
WHERE
    mapping_id = $16
RETURNING
    mapping_id,
    trip_type,
    source_office_id,
    destination_office_id,
    trip_start_date,
    trip_start_time,
    schedule_id,
    schedule_name,
    vehicle_reg_number,
    purpose_for_trip,
    trip_start_reading,
    trip_status,
    long_distance,
    driver_name_prim,
    driving_license_number_prim,
    driver_mobile_number_prim,
    driver_name_sec,
    driving_license_number_sec,
    driver_mobile_number_sec,
    mapping_status,
    created_date,
    created_by,
    updated_by,
    updated_date,
    trip_start_by_office_id,
    source_facility_name,
    destination_facility_name;


UPDATE carriermgmt.schedule_vehicle_driver_mapping
SET
    trip_type = 'FTL',
    destination_office_id = '29460002',
    destination_facility_name = 'CHN CEPT',
    vehicle_reg_number = 'TN22AS1234',
    purpose_for_trip = 'CEPT testing',
    trip_start_reading = 1555,
    long_distance = true,
    driver_name_prim = 'Pruthi R',
    driving_license_number_prim = 'TA0920224567899',
    driver_mobile_number_prim = 9876543210,
    driver_name_sec = "Chauhan',
    driving_license_number_sec = 'TA0920224567899',
    driver_mobile_number_sec = 9876543210,
    mapping_status = 'MAPPED',
    updated_by = 'MAPPED',
    updated_date = 21070745
WHERE
    mapping_id = '2024-03-13T20:43:20Z'
RETURNING
    mapping_id,
    trip_type,
    source_office_id,
    destination_office_id,
    trip_start_date,
    trip_start_time,
    schedule_id,
    schedule_name,
    vehicle_reg_number,
    purpose_for_trip,
    trip_start_reading,
    trip_status,
    long_distance,
    driver_name_prim,
    driving_license_number_prim,
    driver_mobile_number_prim,
    driver_name_sec,
    driving_license_number_sec,
    driver_mobile_number_sec,
    mapping_status,
    created_date,
    created_by,
    updated_by,
    updated_date,
    trip_start_by_office_id,
    source_facility_name,
    destination_facility_name
UPDATE carriermgmt.schedule_vehicle_driver_mapping
SET
    trip_type = 'FTL',
    destination_office_id = '29460002',
    destination_facility_name = 'CHN CEPT',
    vehicle_reg_number = 'TN22AS1234',
    purpose_for_trip = 'CEPT testing',
    trip_start_reading = 1555,
    long_distance = TRUE,
    driver_name_prim = 'Pruthi R',
    driving_license_number_prim = 'TA0920224567899',
    driver_mobile_number_prim = 9876543210,
    driver_name_sec = 'Chauhan', -- Corrected single quotes around 'Chauhan'
    driving_license_number_sec = 'TA0920224567899',
    driver_mobile_number_sec = 9876543210,
    mapping_status = 'MAPPED',
    updated_by = 'MAPPED',
    updated_date = '2025-07-22' -- Corrected date format to 'YYYY-MM-DD'
WHERE
    mapping_id = 31
RETURNING
    mapping_id,
    trip_type,
    source_office_id,
    destination_office_id,
    trip_start_date,
    trip_start_time,
    schedule_id,
    schedule_name,
    vehicle_reg_number,
    purpose_for_trip,
    trip_start_reading,
    trip_status,
    long_distance,
    driver_name_prim,
    driving_license_number_prim,
    driver_mobile_number_prim,
    driver_name_sec,
    driving_license_number_sec,
    driver_mobile_number_sec,
    mapping_status,
    created_date,
    created_by,
    updated_by,
    updated_date,
    trip_start_by_office_id,
    source_facility_name,
    destination_facility_name;
    
    
    WITH trip_data AS (
    SELECT
        svdm.schedule_id,
        svdm.schedule_name,
        svdm.vehicle_reg_number,
        svdm.mapping_id,
        svdm.trip_start_date,
        svdm.trip_end_date,
        (svdm.trip_start_date::timestamp + svdm.trip_start_time) AS trip_start_ts,
        (svdm.trip_end_date::timestamp + svdm.trip_end_time) AS trip_end_ts
    FROM carriermgmt.schedule_vehicle_driver_mapping svdm
    WHERE
        (svdm.driving_license_number_prim = 'TA0920224567899'
         OR svdm.driving_license_number_sec = 'TA0920224567899')
        AND svdm.trip_start_date = '2024-03-13'::date
),
calculated_times AS (
    SELECT
        td.schedule_id,
        td.schedule_name,
        td.vehicle_reg_number,
        td.mapping_id,
        td.trip_start_date,
        td.trip_end_date,
        td.trip_start_ts,
        td.trip_end_ts,
        (td.trip_end_ts - td.trip_start_ts) AS duration,
        -- Calculate night hours (10PM-6AM) more reliably
        -- Function to calculate overlap between two time ranges
        -- This function `get_overlap_seconds` is illustrative. You might need to create it
        -- or inline its logic if your PostgreSQL version doesn't support something similar.
        -- For simplicity here, I'm providing a common approach.

        -- Sum of night hours from start_ts to end_ts
        -- This assumes night hours are from 22:00 to 06:00 (next day)
        CASE
            WHEN td.trip_start_ts IS NULL OR td.trip_end_ts IS NULL THEN INTERVAL '0 seconds'
            ELSE
                (
                    SELECT SUM(
                        EXTRACT(EPOCH FROM (
                            LEAST(range_end, overlap_end) - GREATEST(range_start, overlap_start)
                        ))
                    ) * INTERVAL '1 second'
                    FROM (
                        SELECT
                            generate_series(
                                date_trunc('day', td.trip_start_ts),
                                date_trunc('day', td.trip_end_ts) + INTERVAL '1 day',
                                INTERVAL '1 day'
                            ) AS day_start,
                            td.trip_start_ts AS range_start,
                            td.trip_end_ts AS range_end
                    ) AS s,
                    LATERAL (
                        SELECT
                            s.day_start + INTERVAL '22 hours' AS overlap_start,
                            s.day_start + INTERVAL '1 day' + INTERVAL '6 hours' AS overlap_end
                    ) AS o
                    WHERE
                        GREATEST(range_start, overlap_start) < LEAST(range_end, overlap_end)
                )
        END AS night_hours_interval

    FROM trip_data td
)
SELECT
    schedule_id,
    schedule_name,
    vehicle_reg_number,
    mapping_id,
    trip_start_date,
    trip_end_date,
    trip_start_ts AS trip_start_time,
    trip_end_ts AS trip_end_time,
    duration,
    GREATEST(night_hours_interval, INTERVAL '0') AS night_hours,
    -- Calculate night coefficient in minutes: (night_hours_in_seconds / 3600) * 10
    ROUND(EXTRACT(EPOCH FROM GREATEST(night_hours_interval, INTERVAL '0')) / 3600.0 * 10, 2) AS night_coefficient_minutes,
    -- Calculate total duration with coefficient: duration + night_coefficient_minutes * 1 minute
    duration + (ROUND(EXTRACT(EPOCH FROM GREATEST(night_hours_interval, INTERVAL '0')) / 3600.0 * 10) * INTERVAL '1 minute') AS total_duration_with_coefficient
FROM calculated_times
ORDER BY mapping_id DESC;
    
    WITH trip_data AS (
    SELECT
        svdm.schedule_id,
        svdm.schedule_name,
        svdm.vehicle_reg_number,
        svdm.mapping_id,
        svdm.trip_start_date,
        svdm.trip_end_date,
        (svdm.trip_start_date::timestamp + svdm.trip_start_time) AS trip_start_ts,
        (svdm.trip_end_date::timestamp + svdm.trip_end_time) AS trip_end_ts
    FROM carriermgmt.schedule_vehicle_driver_mapping svdm
    WHERE
        (
            svdm.driving_license_number_prim = :dl_id
            OR svdm.driving_license_number_sec = :dl_id
        )
        AND svdm.trip_start_date >= :from_date::date
        AND svdm.trip_start_date <= :to_date::date
)
, calculated_times AS (
    SELECT
        td.schedule_id,
        td.schedule_name,
        td.vehicle_reg_number,
        td.mapping_id,
        td.trip_start_date,
        td.trip_end_date,
        td.trip_start_ts,
        td.trip_end_ts,
        (td.trip_end_ts - td.trip_start_ts) AS duration,
        -- Calculate night hours: 22:00 to 06:00 logic as before
        CASE
            WHEN td.trip_start_ts IS NULL OR td.trip_end_ts IS NULL THEN INTERVAL '0 seconds'
            ELSE (
                SELECT SUM(
                    EXTRACT(EPOCH FROM (
                        LEAST(range_end, overlap_end) - GREATEST(range_start, overlap_start)
                    ))
                ) * INTERVAL '1 second'
                FROM (
                    SELECT
                        generate_series(
                            date_trunc('day', td.trip_start_ts),
                            date_trunc('day', td.trip_end_ts) + INTERVAL '1 day',
                            INTERVAL '1 day'
                        ) AS day_start,
                        td.trip_start_ts AS range_start,
                        td.trip_end_ts AS range_end
                ) AS s,
                LATERAL (
                    SELECT
                        s.day_start + INTERVAL '22 hours' AS overlap_start,
                        s.day_start + INTERVAL '1 day' + INTERVAL '6 hours' AS overlap_end
                ) AS o
                WHERE
                    GREATEST(range_start, overlap_start) < LEAST(range_end, overlap_end)
            )
        END AS night_hours_interval
    FROM trip_data td
)
SELECT
    schedule_id,
    schedule_name,
    vehicle_reg_number,
    mapping_id,
    trip_start_date,
    trip_end_date,
    trip_start_ts AS trip_start_time,
    trip_end_ts AS trip_end_time,
    duration,
    GREATEST(night_hours_interval, INTERVAL '0') AS night_hours,
    ROUND(EXTRACT(EPOCH FROM GREATEST(night_hours_interval, INTERVAL '0')) / 3600.0 * 10, 2) AS night_coefficient_minutes,
    duration + (ROUND(EXTRACT(EPOCH FROM GREATEST(night_hours_interval, INTERVAL '0')) / 3600.0 * 10) * INTERVAL '1 minute') AS total_duration_with_coefficient
FROM calculated_times
ORDER BY mapping_id DESC;
