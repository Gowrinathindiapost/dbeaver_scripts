select * from carriermgmt.schedule where schedule_id='2803' not in (select schedule_id from carriermgmt.schedule_stop_sequence sss ) and schedule_status='ACTIVE'

INSERT INTO carriermgmt.schedule 
(schedule_id, schedule_name, schedule_type, bag_type, source_facility_id, source_facility_name, destination_facility_id, destination_facility_name, 
schedule_start_time, schedule_create_office_id, schedule_valid_from, schedule_valid_to, transport_mode, schedule_running_days, schedule_status, 
created_by,created_date) 
--, updated_by, updated_date, deleted_by, deleted_date) 
VALUES(nextval('carriermgmt.schedule_schedule_id_seq'::regclass), 
'Chennai CEPT_Mysore CEPT_23:45_KAVERIEXPRES_AI_16027_WEEKLY_Train', 
'Train', 
ARRAY['Speed Post'], 
600002, 
'Chennai CEPT', 
570011, 
'Mysore CEPT', 
'23:45', 
21610000, 
'2023-12-27T00:00:00Z000', 
'2026-02-19T18:30:00Z000', 
'DoP', 
ARRAY['Thursday'], 
'ACTIVE', 
'Maker','2025-07-01T18:30:00Z000');
'', '', '', '', '');

select * from carriermgmt.schedule where schedule_id in ('2805','2806','2807','2734')
select * from carriermgmt.schedule where schedule_status='PENDING'
select * from carriermgmt.schedule_approval_detail where schedule_id='2734' schedule_status='PENDING'
select * from carriermgmt.kafka_office_hierarchy_master where office_id='21960001' circle_code='CR29000000000'

SELECT
  sad.request_number,
  sad.schedule_id,
  s.schedule_name,
  s.schedule_type ,
  s.bag_type ,s.source_facility_id ,
  s.source_facility_name ,s.destination_facility_id ,s.destination_facility_name ,
  s.schedule_start_time ,s.schedule_create_office_id ,s.schedule_valid_from ,s.schedule_valid_to ,
  s.transport_mode ,s.schedule_running_days ,s.schedule_status ,s.updated_by ,s.updated_date ,s.deleted_by ,s.deleted_date ,
  sad.circle_code,
  sad.created_by,
  sad.created_date,
  sad.updated_by,
  sad.updated_date,
  sad.approval_status,
  sad.approval_remarks,
  sad.reject_remarks,
  sad.reconcile_status
FROM carriermgmt.schedule_approval_detail sad
JOIN carriermgmt.schedule s
  ON s.schedule_id = sad.schedule_id
WHERE
  sad.circle_code = 'CR30000000000' AND sad.approval_status = 'PENDING';
		
		Where(sq.Eq{"circle_code": circleCode, "approval_status": "PENDING"}).
		OrderBy("created_date DESC").
		
		
		
		------------------------------7072025
		
		select * from carriermgmt.schedule_vehicle_driver_mapping
		select distinct(trip_status) from carriermgmt.schedule_vehicle_driver_mapping
		
		select * from carriermgmt.vehicle v 
		where v.vehicle_reg_number not in (select b.vehicle_reg_number from (select DISTINCT ON (vehicle_reg_number) 
			vehicle_reg_number, trip_status FROM carriermgmt.schedule_vehicle_driver_mapping ORDER BY vehicle_reg_number, mapping_id DESC) b where b.trip_status ='IN PROGRESS' )
			" +
			"b.trip_status = 'IN PROGRESS')").
		Where(sq.Eq{"v.maintenance_mms": OfficeID, "v.vehicle_status": "ACTIVE"}).
)
		query := dblib.Psql.Select(vehicleTableColumns8 +
		vehicleTableColumns9 +
		vehicleTableColumns10 +
		"puc_valid_upto ,fitness_valid_upto,vehicle_category,acquisition_value,acquisition_date," +
		vehicleTableColumns11 +
		vehicleTableColumns12 +
		vehicleTableColumns6 +
		vehicleTableColumns7).
		From("carriermgmt.vehicle as v").
		// Where("vehicle_reg_number not in( select b.vehicle_reg_number from carriermgmt.vehicle_driver_mapping b)").
		// Where("vehicle_reg_number not in( select b.vehicle_reg_number from carriermgmt.schedule_vehicle_driver_mapping b)").
		Where("vehicle_reg_number not in( select b.vehicle_reg_number from (select DISTINCT ON (vehicle_reg_number)" +
			"vehicle_reg_number, trip_status FROM carriermgmt.schedule_vehicle_driver_mapping ORDER BY vehicle_reg_number, mapping_id DESC) b where " +
			"b.trip_status = 'IN PROGRESS')").
		Where(sq.Eq{"v.maintenance_mms": OfficeID, "v.vehicle_status": "ACTIVE"}).
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		--------------------------------------------------------------------KA12NM0011
		WITH LatestCompletedTripReading AS (
    -- This CTE finds the trip_end_reading for the latest COMPLETED trip for each vehicle.
    -- It uses ROW_NUMBER to assign a rank to each trip within a vehicle's history,
    -- ordered by trip end date/time (and mapping_id for tie-breaking) in descending order.
    -- We only consider trips that have a 'COMPLETED' status.
    SELECT
        svdm.vehicle_reg_number,
        svdm.trip_end_reading,
        ROW_NUMBER() OVER (PARTITION BY svdm.vehicle_reg_number ORDER BY svdm.trip_end_date DESC, svdm.trip_end_time DESC, svdm.mapping_id DESC) as rn
    FROM
        carriermgmt.schedule_vehicle_driver_mapping svdm
    WHERE
        svdm.trip_status = 'COMPLETE'
),
LatestVehicleStatus AS (
    -- This CTE determines the absolute latest trip status for each vehicle.
    -- It ranks all trips for a vehicle by their mapping_id in descending order
    -- to find the most recent entry, regardless of its status.
    SELECT
        svdm.vehicle_reg_number,
        svdm.trip_status,
        ROW_NUMBER() OVER (PARTITION BY svdm.vehicle_reg_number ORDER BY svdm.mapping_id DESC) as rn_status
    FROM
        carriermgmt.schedule_vehicle_driver_mapping svdm
)
-- The main query selects all columns from the vehicle table.
SELECT
    v.*,
    -- It then joins with LatestCompletedTripReading to get the trip_end_reading
    -- for the most recent completed trip (where rn = 1).
    lctr.trip_end_reading AS latest_completed_trip_end_reading
FROM
    carriermgmt.vehicle v
LEFT JOIN
    LatestCompletedTripReading lctr ON v.vehicle_reg_number = lctr.vehicle_reg_number AND lctr.rn = 1
WHERE
    -- The WHERE clause filters out vehicles whose latest overall trip status is 'IN PROGRESS'.
    -- This ensures that only vehicles that are not currently active are returned.
    v.vehicle_reg_number NOT IN (
        SELECT lvs.vehicle_reg_number
        FROM LatestVehicleStatus lvs
        WHERE lvs.rn_status = 1 AND lvs.trip_status = 'IN PROGRESS'
    );

		
		
		CA01CA0102
TN03AB7655
TN72PM2325
NK01KA0001
TN72PM1611
TN01CS9043
TN72PM1239
TN72PM1282
TN72PM8351
TN72PM3627
TN67PS0174
TN72PM5421
TN72PM8186
TN72PM5915
TN72PM1064
TN72PM6513
TN72PM3967
TN72PM3186
KA23EN7522
TN72PM4191
TN72PM5059
TN72PM2653
TN72PM2929
TN72PM2608
TN72PM4621
KM01CH0101
NK01KA0999
ka01wn1123
TN22AS1234
MH02DG3459
TN06CC8045
TN06558045
TN42CR4183
TN72PM8780
TN72PM3047
TN72PM6521
TN22DK1515
TN72PM2775
TN72PM4211
TN72PM2589
TN72PM1184
TN72PM6596
KA01AF0099
SK01KA0101
AP20BK2037
TN05DK8798
TN72PM6245
TN72PM8704
TN72PM1826
TN72PM9088
TS05FN4230
TN72PM6730
TN72PM4439
AP24AK0380
AP24PK3290
		
		
		
		WITH LatestCompletedTripReading AS (
    -- This CTE finds the trip_end_reading for the latest COMPLETED trip for each vehicle.
    -- It uses ROW_NUMBER to assign a rank to each trip within a vehicle's history,
    -- ordered by trip end date/time (and mapping_id for tie-breaking) in descending order.
    -- We only consider trips that have a 'COMPLETED' status.
    SELECT
        svdm.vehicle_reg_number,
        svdm.trip_end_reading,
        ROW_NUMBER() OVER (PARTITION BY svdm.vehicle_reg_number ORDER BY svdm.trip_end_date DESC, svdm.trip_end_time DESC, svdm.mapping_id DESC) as rn
    FROM
        carriermgmt.schedule_vehicle_driver_mapping svdm
    WHERE
        svdm.trip_status in ('COMPLETE','END')
),
LatestVehicleStatus AS (
    -- This CTE determines the absolute latest trip status for each vehicle.
    -- It ranks all trips for a vehicle by their mapping_id in descending order
    -- to find the most recent entry, regardless of its status.
    SELECT
        svdm.vehicle_reg_number,
        svdm.trip_status,
        ROW_NUMBER() OVER (PARTITION BY svdm.vehicle_reg_number ORDER BY svdm.mapping_id DESC) as rn_status
    FROM
        carriermgmt.schedule_vehicle_driver_mapping svdm
)
-- The main query selects all columns from the vehicle table.
SELECT
    v.*,
    -- It then joins with LatestCompletedTripReading to get the trip_end_reading
    -- for the most recent completed trip (where rn = 1).
    lctr.trip_end_reading AS latest_completed_trip_end_reading
FROM
    carriermgmt.vehicle v
LEFT JOIN
    LatestCompletedTripReading lctr ON v.vehicle_reg_number = lctr.vehicle_reg_number AND lctr.rn = 1
WHERE
    -- Added conditions for maintenance_mms and vehicle_status
  --  v.maintenance_mms = '21610000' AND v.vehicle_status = 'ACTIVE' AND
    -- The WHERE clause filters out vehicles whose latest overall trip status is 'IN PROGRESS'.
    -- This ensures that only vehicles that are not currently active are returned.
    v.vehicle_reg_number NOT IN (
        SELECT lvs.vehicle_reg_number
        FROM LatestVehicleStatus lvs
        WHERE lvs.rn_status = 1 AND lvs.trip_status = 'IN PROGRESS'
    );
--MH01JK1234 MP09AB1235 NK01KA0001 TN22DK2008 TN87PS0187

select * from carriermgmt.schedule where schedule_id in ('119767','119768')  
select * from carriermgmt.schedule_stop_sequence where schedule_id in ('119767','119768')
--16300001
select * from carriermgmt.kafka_office_hierarchy_master where office_id='16300001'


 WITH LatestCompletedTripReading AS (
    SELECT
        svdm.vehicle_reg_number,
        svdm.trip_end_reading,
        ROW_NUMBER() OVER (PARTITION BY svdm.vehicle_reg_number ORDER BY svdm.trip_end_date DESC, svdm.trip_end_time DESC, svdm.mapping_id DESC) as rn
    FROM
        carriermgmt.schedule_vehicle_driver_mapping svdm
    WHERE
        svdm.trip_status in ('COMPLETE','END')
),
LatestVehicleStatus AS (
    SELECT
        svdm.vehicle_reg_number,
        svdm.trip_status,
        ROW_NUMBER() OVER (PARTITION BY svdm.vehicle_reg_number ORDER BY svdm.mapping_id DESC) as rn_status
    FROM
        carriermgmt.schedule_vehicle_driver_mapping svdm
)
SELECT
    v.vehicle_reg_number,
	v.reg_valid_upto,
	v.vehicle_type,
	v.weight_category,
	v.contractor_id,
	COALESCE(lctr.trip_end_reading, v.present_meter_reading) AS present_meter_reading,
	v.manufacturer,
	v.manufacturer_country,
	v.model_number,
	v.manufacturer_year,
	v.insurance_valid_upto,
	v.puc_valid_upto,
	v.fitness_valid_upto,
	v.vehicle_category,
	v.acquisition_value,
	v.acquisition_date,
	v.maintenance_date,
	v.maintenance_mms,
	v.operational_circle,
	v.operational_office,
	v.engine_number,
	v.chasis_number,
	v.max_load_weight,
	v.max_load_volume,
	v.primary_fuel,
	v.emission_standards,
	v.maintenance_interval_km,
	v.vehicle_status,
	v.created_by,
	v.created_date,
	v.updated_by,
	v.updated_date,
	v.deleted_by,
	v.deleted_date
	
    
FROM
    carriermgmt.vehicle v
LEFT JOIN
    LatestCompletedTripReading lctr ON v.vehicle_reg_number = lctr.vehicle_reg_number AND lctr.rn = 1
WHERE
v.maintenance_mms = '21610000' AND v.vehicle_status = 'ACTIVE' AND
    v.vehicle_reg_number NOT IN (
        SELECT lvs.vehicle_reg_number
        FROM LatestVehicleStatus lvs
        WHERE lvs.rn_status = 1 AND lvs.trip_status = 'IN PROGRESS');
		 LIMIT $2
        OFFSET $3

select * from carriermgmt.schedule_vehicle_driver_mapping where vehicle_reg_number='MP09AB1235'

		