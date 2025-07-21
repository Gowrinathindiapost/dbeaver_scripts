INSERT INTO carriermgmt.schedule_header_stop_sequence (
    schedule_id,
    sequence_number,
    stop_office_id,
    stop_office_name,
    distance_from_prev_stop,
    transit_day,
    transit_hour,
    transit_minute,
    stay_duration_day,
    stay_duration_hour,
    stay_duration_minute,
    departure_time,
    arrival_time,
    created_by,
    created_date,
    office_location_latitude,
    office_location_longitude,
    circle_id,
    section_id,
    section_beat_name,
    station_code,
    station_name
) VALUES (
    102,                  -- schedule_id (example integer)
    1,                    -- sequence_number (example integer)
    61210006,                 -- stop_office_id (example integer)
    'Bangalore Hub',      -- stop_office_name (example string)
    150.75,               -- distance_from_prev_stop (example numeric)
    0,                    -- transit_day (example integer)
    3,                    -- transit_hour (example integer)
    0,                   -- transit_minute (example integer)
    0,                    -- stay_duration_day (example integer)
    3.3,                    -- stay_duration_hour (example integer)
    0,                   -- stay_duration_minute (example integer)
    '10:00:00',           -- departure_time (example time string)
    '06:30:00',           -- arrival_time (example time string)
    'admin_user',         -- created_by (example string)
    now(),                -- created_date (current timestamp)
    12.9716,              -- office_location_latitude (example float)
    77.5946,              -- office_location_longitude (example float)
    'KA',                 -- circle_id (example string)
    'BLR-SEC-A',          -- section_id (example string)
    'Central Beat',       -- section_beat_name (example string)
    'BLR',                -- station_code (example string)
    'Bangalore City'      -- station_name (example string)
);

SELECT
    *
FROM
    carriermgmt.schedule_header_stop_sequences_stg AS s
JOIN
    carriermgmt.kafka_office_master AS kom ON s.stop_office_id = kom.office_id
JOIN
    carriermgmt.kafka_office_hierarchy_master AS kohm ON kom.office_id = kohm.office_id
WHERE
    s.approval_status = false or s.approval_status = null -- Replaced consts.ACTIVE with 'ACTIVE'
    AND kohm.circle_code::TEXT = ( --select * FROM carriermgmt.kafka_office_hierarchy_master WHERE  office_id='31102084'
        SELECT
            kohm_inner.circle_code
        FROM
            carriermgmt.kafka_office_hierarchy_master kohm_inner
        WHERE
            kohm_inner.office_id = '29140000'
--21360043 -- Replaced ScheduleCreateOfficeId with an example ID
    )
    AND s.schedule_type ILIKE ANY(ARRAY['MMS%', 'VEHICLE%'])
    AND NOT EXISTS (
        SELECT
            1
        FROM
            carriermgmt.schedule_vehicle_driver_mapping AS b
        WHERE
            b.schedule_id = s.schedule_id
            AND b.trip_status = 'IN PROGRESS'
    )
ORDER BY
    s.schedule_id
LIMIT 10 -- Replaced reqMetadata.Limit with an example limit
OFFSET 0; -- Replaced reqMetadata.Skip * reqMetadata.Limit with an example offset (e.g., 0 for skip=0, limit=10)
29960001
21360043
SELECT * FROM carriermgmt.kafka_office_hierarchy_master kohm_inner where office_id='29960001'
select * from carriermgmt.kafka_office_hierarchy_master kohm_inner where division_name ILIKE %yakuma%;
SELECT *
FROM carriermgmt.kafka_office_hierarchy_master kohm_inner
WHERE division_name ILIKE '%yakuma%';

SELECT
    *
FROM
    carriermgmt.schedule_header_stop_sequences_stg AS s
JOIN
    carriermgmt.kafka_office_master AS kom ON s.stop_office_id = kom.office_id
JOIN
    carriermgmt.kafka_office_hierarchy_master AS kohm ON kom.office_id = kohm.office_id
WHERE
    s.approval_status = FALSE OR s.approval_status IS null
    AND kohm.circle_code::TEXT = (
        SELECT
            kohm_inner.circle_code
        FROM
            carriermgmt.kafka_office_hierarchy_master kohm_inner
        WHERE
            kohm_inner.office_id = '21360043'

    )