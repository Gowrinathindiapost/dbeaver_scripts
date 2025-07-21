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
   -- (duration + (ROUND(EXTRACT(EPOCH FROM night_hours)/3600 * 10) * INTERVAL '1 minute') AS total_duration_with_coefficient
    (duration + (ROUND(EXTRACT(EPOCH FROM night_hours)/3600 * 10) * INTERVAL '1 minute')) AS total_duration_with_coefficient

FROM 
    trip_data
ORDER BY 
    mapping_id DESC;