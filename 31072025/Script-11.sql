ALTER TABLE carriermgmt.schedule_vehicle_driver_mapping ALTER COLUMN schedule_name DROP NOT NULL;
ALTER TABLE carriermgmt.schedule_vehicle_driver_mapping ALTER COLUMN schedule_id SET NOT NULL;