SELECT 
    s.schedule_id, 
    s.schedule_name,
    s.schedule_type,
    s.bag_type,
    s.source_facility_id,
    s.source_facility_name,
    s.destination_facility_id,
    s.destination_facility_name,
    s.schedule_start_time,
    s.schedule_create_office_id,
    s.schedule_valid_from,
    s.schedule_valid_to,
    s.transport_mode,
    s.schedule_running_days,
    s.schedule_status,
    s.created_by,
    s.created_date,
    s.updated_by,
    s.updated_date
FROM 
    carriermgmt.schedule AS s
JOIN 
    carriermgmt.kafka_office_master AS kom 
    ON s.source_facility_id = kom.office_id
JOIN 
    carriermgmt.kafka_office_hierarchy_master AS kohm 
    ON kom.office_id = kohm.office_id
WHERE 
    s.schedule_status = 'ACTIVE'::carriermgmt.schedule_status_enum
    AND kohm.circle_code::TEXT = (
        SELECT kohm_inner.circle_code
        FROM carriermgmt.kafka_office_hierarchy_master kohm_inner
        WHERE kohm_inner.office_id = 21630000
    )
    AND s.schedule_type ILIKE ANY (ARRAY['MMS%', 'VEHICLE%'])
    AND not EXISTS (
        SELECT 1
        FROM carriermgmt.schedule_vehicle_driver_mapping AS b 
        where b.schedule_id = s.schedule_id and  
        b.trip_status = 'IN PROGRESS' -- '<> 'COMPLETE'
        --AND b.schedule_name IS NOT NULL
    )
ORDER BY 
    s.schedule_id
LIMIT 2147483647
OFFSET 0;


SELECT trip_status
        FROM carriermgmt.schedule_vehicle_driver_mapping vt
        WHERE vt.vehicle_reg_number = $1
        ORDER BY mapping_id DESC
        LIMIT 1
        FOR UPDATE