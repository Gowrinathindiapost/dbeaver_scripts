
CREATE TABLE carriermgmt.diesel_issue_register (
	register_id serial4 NOT NULL,
	vehicle_reg_number varchar(20) NOT NULL,
	last_fuel_km float8 NOT NULL,
	current_fuel_km float8 NULL,
	kmpl float null,
	distance_covered float8 NULL,
	opening_pump_reading float8 not null,
	closing_pump_reading float8 not null,
	fuel_filled_in_liters float8 not null,
	driver_name_prim varchar(20) NOT NULL,
	driver_name_sec varchar(20) NULL,
	created_by varchar(50) NOT NULL,
	created_date timestamp NOT NULL,
	updated_by varchar(50) NULL,
	updated_date timestamp NULL,
	deleted_by varchar(50) NULL,
	deleted_date timestamp NULL,
	CONSTRAINT diesel_issue_register_pkey PRIMARY KEY (register_id)
);

-- Permissions

ALTER TABLE carriermgmt.gate_register OWNER TO postgres;
GRANT ALL ON TABLE carriermgmt.gate_register TO postgres;