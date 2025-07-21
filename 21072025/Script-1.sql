select * from carriermgmt.driver where carriermgmt.driver.driver_onboarding_office_id ='21960001'
select * from carriermgmt.driver where carriermgmt.driver.driving_license_number  ='TA0920224567899'

SELECT d.driving_license_number, d.driver_first_name, d.driver_last_name, 
       d.driver_type, d.vehicle_type, d.driving_certifications,
       d.aadhaar_number, d.driver_phone_number, d.date_of_birth, 
       d.contractor_id, d.employee_id, d.driver_status, d.driver_onboarding_office_id,
       d.created_by, d.created_date, d.updated_by, d.updated_date, 
       d.deleted_by, d.deleted_date, d.driving_license_validity_date
FROM carriermgmt.driver AS d
WHERE d.driver_onboarding_office_id = '21960001'
  AND d.driving_license_number NOT IN (
      SELECT b.driving_license_number_prim 
      FROM carriermgmt.schedule_vehicle_driver_mapping AS b
  )
  AND d.driving_license_number NOT IN (
      SELECT b.driving_license_number_sec 
      FROM carriermgmt.schedule_vehicle_driver_mapping AS b
  )
ORDER BY d.driving_license_number
LIMIT $2 OFFSET $3;

SELECT d.driving_license_number, d.driver_first_name, d.driver_last_name, 
       d.driver_type, d.vehicle_type, d.driving_certifications,
       d.aadhaar_number, d.driver_phone_number, d.date_of_birth, 
       d.contractor_id, d.employee_id, d.driver_status, d.driver_onboarding_office_id,
       d.created_by, d.created_date, d.updated_by, d.updated_date, 
       d.deleted_by, d.deleted_date, d.driving_license_validity_date
FROM carriermgmt.driver AS d
WHERE d.driver_onboarding_office_id = '21960001'
  AND NOT EXISTS (
      SELECT 1 
      FROM carriermgmt.schedule_vehicle_driver_mapping AS b
      WHERE b.driving_license_number_prim = d.driving_license_number
  )
  AND NOT EXISTS (
      SELECT 1 
      FROM carriermgmt.schedule_vehicle_driver_mapping AS b
      WHERE b.driving_license_number_sec = d.driving_license_number
  )
ORDER BY d.driving_license_number;

SELECT 1 FROM carriermgmt.vehicle_driver_mapping AS b where b.driving_license_number_prim  = d.driving_license_number or b.driving_license_number_sec = d.driving_license_number


 SELECT s.schedule_id, s.schedule_name,s.schedule_type,s.bag_type,
		s.source_facility_id,s.source_facility_name,s.destination_facility_id,
		s.destination_facility_name,s.schedule_start_time,s.schedule_create_office_id,
		s.schedule_valid_from,s.schedule_valid_to,s.transport_mode,s.schedule_running_days,
		s.schedule_status,s.created_by,s.created_date,s.updated_by,s.updated_date
        FROM carriermgmt.schedule AS s
        JOIN carriermgmt.kafka_office_master AS kom ON s.source_facility_id = kom.office_id
        JOIN carriermgmt.kafka_office_hierarchy_master AS kohm ON kom.office_id = kohm.office_id
        WHERE s.schedule_status = 'ACTIVE'--$1::schedule_status_enum
        AND kohm.circle_code::TEXT = (
            SELECT kohm_inner.circle_code
            FROM carriermgmt.kafka_office_hierarchy_master kohm_inner
            WHERE kohm_inner.office_id = $2
        )
			--AND s.schedule_type ILIKE ANY (ARRAY[$3, $4])
        AND s.schedule_type ILIKE ANY (ARRAY['MMS%', 'VEHICLE%'])

			--AND s.schedule_name NOT IN (
     			-- SELECT b.schedule_name 
      		--FROM carriermgmt.schedule_vehicle_driver_mapping AS b
  		--)
        ORDER BY s.schedule_id

        
        
         SELECT s.schedule_id, s.schedule_name,s.schedule_type,s.bag_type,
		s.source_facility_id,s.source_facility_name,s.destination_facility_id,
		s.destination_facility_name,s.schedule_start_time,s.schedule_create_office_id,
		s.schedule_valid_from,s.schedule_valid_to,s.transport_mode,s.schedule_running_days,
		s.schedule_status,s.created_by,s.created_date,s.updated_by,s.updated_date
        FROM carriermgmt.schedule AS s
        JOIN carriermgmt.kafka_office_master AS kom ON s.source_facility_id = kom.office_id
        JOIN carriermgmt.kafka_office_hierarchy_master AS kohm ON kom.office_id = kohm.office_id
        WHERE s.schedule_status = 'ACTIVE'--$1::schedule_status_enum
        AND kohm.circle_code::TEXT = (
            SELECT kohm_inner.circle_code
            FROM carriermgmt.kafka_office_hierarchy_master kohm_inner
            WHERE kohm_inner.office_id = $2
        )
			--AND s.schedule_type ILIKE ANY (ARRAY[$3, $4])
			AND s.schedule_name NOT IN (
     			 SELECT b.schedule_name 
      		FROM carriermgmt.schedule_vehicle_driver_mapping AS b 
			--WHERE b.trip_status <> 'COMPLETE'
  		)
        ORDER BY s.schedule_id
        LIMIT $5
        OFFSET $6
        
        SELECT office_type_code FROM carriermgmt.kafka_office_hierarchy_master WHERE office_id = '21260000'