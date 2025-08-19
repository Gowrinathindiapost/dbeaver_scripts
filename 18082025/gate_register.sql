CREATE TABLE carriermgmt.gate_register (
	register_id serial4 not null,
	schedule_id int4 NULL,--
	schedule_name varchar(255) NULL,--
	vehicle_reg_number varchar(20) not null,
	starting_km float8 not null,
	closing_km float8  null,
	distance_covered float8 null,
	driver_name_prim varchar(20) not null,
	driver_name_sec varchar(20) null,
	created_by varchar(50) NOT NULL,--
	created_date timestamp NOT NULL,--
	updated_by varchar(50) NULL,--
	updated_date timestamp NULL,--
	deleted_by varchar(50) NULL,
	deleted_date timestamp NULL,
	CONSTRAINT gate_register_pkey PRIMARY KEY (register_id)
);


ALTER TABLE carriermgmt.gate_register ADD CONSTRAINT gate_register_unique UNIQUE (schedule_id,vehicle_reg_number,starting_km);
ALTER TABLE carriermgmt.gate_register ADD CONSTRAINT gate_register_unique_1 UNIQUE (actual_arrival_time,schedule_id,vehicle_reg_number,closing_km);
