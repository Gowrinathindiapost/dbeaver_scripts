--old query below
CREATE TABLE carriermgmt.rail (
	rail_id bigserial NOT NULL,
	train_number int4 NOT NULL,
	from_station varchar(20) NOT NULL,
	to_station varchar(20) NOT NULL,
	seating_capacity_contract int8 NOT NULL,
	rail_status carriermgmt."rail_status_enum" NOT NULL,
	created_by varchar(50) NOT NULL,
	updated_by varchar(50) NULL,
	deleted_by varchar(50) NULL,
	created_date timestamp NULL,
	updated_date timestamp NULL,
	deleted_date timestamp NULL,
	contract_from_date timestamp NOT NULL,
	contract_to_date timestamp NOT NULL,
	rail_onboard_office_id int8 NOT NULL,
	CONSTRAINT rail_on_board_pkey PRIMARY KEY (train_number)
);
------------old query above
-- Step 1: Drop Primary Key Constraint
ALTER TABLE carriermgmt.rail DROP CONSTRAINT rail_on_board_pkey;

-- Step 2: Alter Column Type from INT4 to VARCHAR(10)
ALTER TABLE carriermgmt.rail ALTER COLUMN train_number TYPE VARCHAR(10);

-- Step 3: Recreate Primary Key Constraint
ALTER TABLE carriermgmt.rail ADD CONSTRAINT rail_on_board_pkey PRIMARY KEY (train_number);
