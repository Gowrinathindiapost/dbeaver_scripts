select * from carriermgmt.vehicle v  where 
v.vehicle_id not in (select * from carriermgmt.vehicle_driver_mapping)

SELECT v.vehicle_reg_number 
FROM carriermgmt.vehicle v
LEFT JOIN carriermgmt.vehicle_driver_mapping vdm 
    ON v.vehicle_reg_number = vdm.vehicle_reg_number
WHERE vdm.vehicle_reg_number IS NULL;

SELECT v.vehicle_reg_number,d.driver_first_name,d.driving_license_number 
FROM carriermgmt.vehicle v
LEFT JOIN carriermgmt.vehicle_driver_mapping vdm 
    ON v.vehicle_reg_number = vdm.vehicle_reg_number
LEFT JOIN carriermgmt.driver d 
    ON vdm.driving_license_number = d.driving_license_number
WHERE vdm.vehicle_reg_number IS NULL 
--   OR vdm.driving_license_number IS NULL;
create table schedule_vehicle_driver_mapping
schedule_id 
schedule_name
vehicle_reg_number 
trip_start_date
trip_close_date
driver_name
driving_license_number
driver_mobile_number
mapping_status

CREATE TABLE carriermgmt.schedule_vehicle_driver_mapping (
    mapping_id SERIAL PRIMARY KEY,
    schedule_id INT NOT NULL,
    schedule_name VARCHAR(255) NOT NULL,
    vehicle_reg_number VARCHAR(20) NOT NULL,
    trip_start_date TIMESTAMP NOT NULL,
    trip_close_date TIMESTAMP NULL,
    driver_name VARCHAR(100) NOT NULL,
    driving_license_number VARCHAR(20) NOT NULL,
    driver_mobile_number BIGINT NOT NULL,
    mapping_status VARCHAR(50) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by VARCHAR(50) NOT NULL,
    updated_by VARCHAR(50) NULL,
    updated_date TIMESTAMP NULL,
    deleted_by VARCHAR(50) NULL,
    deleted_date TIMESTAMP NULL,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_schedule FOREIGN KEY (schedule_id) 
        REFERENCES carriermgmt.schedule(schedule_id) ON DELETE CASCADE,
    
    CONSTRAINT fk_vehicle FOREIGN KEY (vehicle_reg_number) 
        REFERENCES carriermgmt.vehicle(vehicle_reg_number) ON DELETE CASCADE,
    
    CONSTRAINT fk_driver FOREIGN KEY (driving_license_number) 
        REFERENCES carriermgmt.driver(driving_license_number) ON DELETE CASCADE
);
----
CREATE TABLE carriermgmt.schedule_vehicle_driver_mapping (
    mapping_id SERIAL PRIMARY KEY,
    schedule_id INT NOT NULL,
    schedule_name VARCHAR(255) NOT NULL,
    vehicle_reg_number VARCHAR(20) NOT NULL,
    trip_start_date TIMESTAMP NOT NULL,
    trip_close_date TIMESTAMP NULL,
    driver_name VARCHAR(100) NOT NULL,
    driving_license_number VARCHAR(20) NOT NULL,
    driver_mobile_number BIGINT NOT NULL,
    mapping_status VARCHAR(50) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by VARCHAR(50) NOT NULL,
    updated_by VARCHAR(50) NULL,
    updated_date TIMESTAMP NULL,
    deleted_by VARCHAR(50) NULL,
    deleted_date TIMESTAMP NULL,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_schedule FOREIGN KEY (schedule_id) 
        REFERENCES carriermgmt.schedule(schedule_id) ,
    
    CONSTRAINT fk_vehicle FOREIGN KEY (vehicle_reg_number) 
        REFERENCES carriermgmt.vehicle(vehicle_reg_number) ,
    
    CONSTRAINT fk_driver FOREIGN KEY (driving_license_number) 
        REFERENCES carriermgmt.driver(driving_license_number) 
);
--multiple drivers
CREATE TABLE carriermgmt.schedule_vehicle_driver_mapping (
    mapping_id SERIAL PRIMARY KEY,
    schedule_id INT NOT NULL,
    schedule_name VARCHAR(255) NOT NULL,
    vehicle_reg_number VARCHAR(20) NOT NULL,
    trip_start_date TIMESTAMP NOT NULL,
    trip_close_date TIMESTAMP NULL,
    long_distance bool null,
    driver_name_prim VARCHAR(100) NOT NULL,
    driving_license_number_prim VARCHAR(20) NOT NULL,
    driver_mobile_number_prim BIGINT NOT NULL,
    driver_name_sec VARCHAR(100)  NULL,
    driving_license_number_sec VARCHAR(20) NULL,
    driver_mobile_number_sec BIGINT NULL,
    mapping_status VARCHAR(50) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by VARCHAR(50) NOT NULL,
    updated_by VARCHAR(50) NULL,
    updated_date TIMESTAMP NULL,
    deleted_by VARCHAR(50) NULL,
    deleted_date TIMESTAMP NULL,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_schedule FOREIGN KEY (schedule_id) 
        REFERENCES carriermgmt.schedule(schedule_id) ,
    
    CONSTRAINT fk_vehicle FOREIGN KEY (vehicle_reg_number) 
        REFERENCES carriermgmt.vehicle(vehicle_reg_number) ,
    
    CONSTRAINT fk_driver_prim FOREIGN KEY (driving_license_number_prim) 
        REFERENCES carriermgmt.driver(driving_license_number),
        CONSTRAINT fk_driver_sec FOREIGN KEY (driving_license_number_sec) 
        REFERENCES carriermgmt.driver(driving_license_number) 
);
---trip start included
CREATE TABLE carriermgmt.schedule_vehicle_driver_mapping (
    mapping_id SERIAL PRIMARY KEY,
    trip_type varchar(20) NULL,
    source_office_id int8 NOT NULL,
	destination_office_id int8 NULL,
	trip_start_date date NOT NULL,
	trip_start_time time NOT NULL,
	schedule_id int4 NOT NULL,
    schedule_name VARCHAR(255) NOT NULL,
    vehicle_reg_number VARCHAR(20) NOT NULL,
    
    purpose_for_trip varchar(100) NULL,
	trip_start_reading float8 NULL,
	trip_status carriermgmt."vehicle_trip_status_enum" NULL,
	trip_end_reading float8 NULL,
	distance_covered float8 NULL,
	trip_end_date date NULL,
	trip_end_time time NULL,
	trip_start_by varchar(50) NULL,
	trip_end_by varchar(50) NULL,
	reason_for_trip_cancel varchar(100) NULL,
	trip_cancel_by varchar(50) NULL,
	trip_cancel_date timestamp NULL,
   
    long_distance bool null,
    driver_name_prim VARCHAR(100) NOT NULL,
    driving_license_number_prim VARCHAR(20) NOT NULL,
    driver_mobile_number_prim BIGINT NOT NULL,
    driver_name_sec VARCHAR(100)  NULL,
    driving_license_number_sec VARCHAR(20) NULL,
    driver_mobile_number_sec BIGINT NULL,
    mapping_status VARCHAR(50) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by VARCHAR(50) NOT NULL,
    updated_by VARCHAR(50) NULL,
    updated_date TIMESTAMP NULL,
    deleted_by VARCHAR(50) NULL,
    deleted_date TIMESTAMP NULL,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_schedule FOREIGN KEY (schedule_id) 
        REFERENCES carriermgmt.schedule(schedule_id) ,
    
    CONSTRAINT fk_vehicle FOREIGN KEY (vehicle_reg_number) 
        REFERENCES carriermgmt.vehicle(vehicle_reg_number) ,
    
    CONSTRAINT fk_driver_prim FOREIGN KEY (driving_license_number_prim) 
        REFERENCES carriermgmt.driver(driving_license_number),
        CONSTRAINT fk_driver_sec FOREIGN KEY (driving_license_number_sec) 
        REFERENCES carriermgmt.driver(driving_license_number) 
);


