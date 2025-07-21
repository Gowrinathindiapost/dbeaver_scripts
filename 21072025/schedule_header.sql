CREATE TABLE carriermgmt.schedule_header (
	schedule_id serial4 NOT NULL,--
	schedule_name varchar(255) NULL,--
	schedule_type varchar(20) NOT NULL,
	schedule_coverage varchar(255) null,
	bag_type _varchar NOT NULL,
	source_office_id int8 NOT NULL,--
	source_office_name varchar(50) NOT NULL,
	destination_office_id int8 NULL,
	destination_office_name varchar(50) NULL,
	schedule_start_time varchar(20) NOT NULL,--
	schedule_reach_time varchar(20) not null,--
	schedule_create_office_id int8 NOT NULL,--
	schedule_valid_from timestamp NOT NULL,--
	schedule_valid_to timestamp NOT NULL,--
	transport_mode varchar(20) NOT NULL,--dop/contractural
	schedule_running_days _varchar NOT NULL,--days
	schedule_status carriermgmt."schedule_status_enum" NOT NULL,--
	approval_status bool null,
	approved_by varchar(50) null,
	created_by varchar(50) NOT NULL,--
	created_date timestamp NOT NULL,--
	updated_by varchar(50) NULL,--
	updated_date timestamp NULL,--
	deleted_by varchar(50) NULL,
	deleted_date timestamp NULL,
	CONSTRAINT schedule_header_pkey PRIMARY KEY (schedule_id)
);