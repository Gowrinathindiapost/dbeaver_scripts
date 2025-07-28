ALTER TABLE carriermgmt.driver
ADD COLUMN driving_license_validity_date date NULL;

SELECT COUNT(*) 
FROM carriermgmt.driver 
WHERE driving_license_validity_date IS NULL;

UPDATE carriermgmt.driver
SET driving_license_validity_date = '2099-12-31'
WHERE driving_license_validity_date IS NULL;

ALTER TABLE carriermgmt.driver 
ALTER COLUMN driving_license_validity_date SET NOT NULL;


SELECT a.schedule_id, a.schedule_name
FROM carriermgmt.schedule AS a
JOIN carriermgmt.carrier_schedule_mapping AS s 
    ON s.schedule_id = a.schedule_id
WHERE a.source_facility_id = '21610000'
    AND a.schedule_status = 'ACTIVE'
    AND s.carrier_type = 'VEHICLE'
ORDER BY a.schedule_id
OFFSET <Skip * Limit>
LIMIT <Limit>;
select * from schedule 

SELECT 
    a.schedule_id, 
    a.schedule_name
FROM carriermgmt.schedule AS a
JOIN carriermgmt.carrier_schedule_mapping AS s 
    ON s.schedule_id = a.schedule_id
JOIN kafka_office_master kom 
    ON a.source_facility_id = kom.office_id 
JOIN kafka_office_hierarchy_master kohm 
    ON kom.office_id = kohm.office_id 
WHERE 
    a.schedule_status = 'ACTIVE'
    AND s.carrier_type = 'VEHICLE'
    AND kohm.circle_code = (
        SELECT kohm_inner.circle_code 
        FROM kafka_office_hierarchy_master kohm_inner
        WHERE kohm_inner.office_id = '21610000'
    )
ORDER BY a.schedule_id;
SELECT unnest(enum_range(NULL::schedule_status_enum));
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'kafka_office_hierarchy_master' AND column_name = 'circle_code';
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'kafka_office_hierarchy_master' AND column_name = 'office_id';

----
SELECT 
    s.schedule_id, 
    s.schedule_name
FROM carriermgmt.schedule AS s
JOIN carriermgmt.schedule_vehicle_driver_mapping AS svdm 
    ON svdm.schedule_id = s.schedule_id
JOIN kafka_office_master kom 
    ON s.schedule_create_office_id = kom.office_id 
JOIN kafka_office_hierarchy_master kohm 
    ON kom.office_id = kohm.office_id 
WHERE 
    s.schedule_status = 'ACTIVE'
    AND kohm.circle_code = (
        SELECT kohm_inner.circle_code 
        FROM kafka_office_hierarchy_master kohm_inner
        WHERE kohm_inner.office_id = '21610000'
    )
ORDER BY s.schedule_id;
---
select * from schedule where schedule_create_office_id ='21610000' and schedule_status='ACTIVE'
SELECT 
    s.schedule_id, 
    s.schedule_name,
    s.schedule_create_office_id ,
    kohm.circle_code,
    s.schedule_status
  FROM carriermgmt.schedule AS s
LEFT JOIN carriermgmt.schedule_vehicle_driver_mapping svdm 
    ON svdm.schedule_id = s.schedule_id
JOIN kafka_office_master kom 
    ON s.schedule_create_office_id = kom.office_id 
JOIN kafka_office_hierarchy_master kohm 
    ON kom.office_id = kohm.office_id 
WHERE 
    s.schedule_status = 'ACTIVE'
    AND kohm.circle_code = (
        SELECT kohm_inner.circle_code 
        FROM kafka_office_hierarchy_master kohm_inner
        WHERE kohm_inner.office_id = '21610000'
    )
    AND svdm.schedule_id IS null -- Ensures schedules in svdm are excluded
    and s.schedule_type= 'MMS Schedule'
ORDER BY s.schedule_id;
select * from schedule where schedule_status='ACTIVE' and schedule
-----------------
SELECT s.schedule_id, s.schedule_name ,s.schedule_create_office_id , s.schedule_status,s.schedule_type,
    kohm.circle_code FROM carriermgmt.schedule AS s 
JOIN kafka_office_master AS kom ON s.source_facility_id = kom.office_id
JOIN kafka_office_hierarchy_master AS kohm ON kom.office_id = kohm.office_id
WHERE (s.schedule_status::text = 'ACTIVE' AND kohm.circle_code = 
(SELECT kohm_inner.circle_code FROM kafka_office_hierarchy_master kohm_inner 
WHERE kohm_inner.office_id = $1)) ORDER BY s.schedule_id

select * from schedule where schedule_id in (1915, 1969, 2027
)

SELECT s.schedule_id, s.schedule_name,s.schedule_type,s.bag_type,
		s.source_facility_id,s.source_facility_name,s.destination_facility_id,
		s.destination_facility_name,s.schedule_start_time,s.schedule_create_office_id,
		s.schedule_valid_from,s.schedule_valid_to,s.transport_mode,s.schedule_running_days,
		s.schedule_status,s.created_by,s.created_date,s.updated_by,s.updated_date
        FROM carriermgmt.schedule AS s
        JOIN carriermgmt.kafka_office_master AS kom ON s.source_facility_id = kom.office_id
        JOIN carriermgmt.kafka_office_hierarchy_master AS kohm ON kom.office_id = kohm.office_id
        WHERE s.schedule_status = 'ACTIVE'::schedule_status_enum
        AND kohm.circle_code::TEXT = (
            SELECT kohm_inner.circle_code
            FROM kafka_office_hierarchy_master kohm_inner
            WHERE kohm_inner.office_id = $2
        )
			--AND s.schedule_type ILIKE ANY (ARRAY[$3, $4])
        ORDER BY s.schedule_id
        LIMIT $5
        OFFSET $6
        select * from schedule where schedule_status='ACTIVE'