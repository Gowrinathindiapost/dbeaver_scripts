SELECT 
  v.mapping_id, 
  v.trip_type,
  v.source_office_id, 
  v.destination_office_id,
  v.trip_start_date::DATE, 
  v.trip_start_time::TIME,
  v.schedule_id, 
  v.schedule_name,
  v.vehicle_reg_number, 
  v.purpose_for_trip,
  v.trip_start_reading, 
  v.trip_status,
  v.trip_start_by, 
  v.long_distance,
  v.driver_name_prim, 
  v.driving_license_number_prim, 
  v.driver_mobile_number_prim,
  v.driving_license_number_sec, 
  v.driver_name_sec, 
  v.driver_mobile_number_sec,
  v.mapping_status, 
  v.trip_start_by_office_id,
  v.source_facility_name, 
  v.destination_facility_name,
  v.created_date, 
  v.created_by, 
  v.updated_by, 
  v.updated_date
FROM 
  carriermgmt.schedule_vehicle_driver_mapping AS v
WHERE 
  (
    --v.destination_office_id = '21960001' OR 
    --v.source_office_id = '21960001' OR 
    v.trip_end_by_office_id = '21960001' OR 
    v.trip_start_by_office_id = '21960001'
  )
  AND v.trip_status = 'COMPLETE'
  AND (
    trip_end_date >= '2025-04-22' OR 
    trip_start_date >= '2025-04-22'
  )
ORDER BY 
  mapping_id
OFFSET 
  10
LIMIT 
  10;
