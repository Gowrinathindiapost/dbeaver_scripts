-- carriermgmt.schedule_header_stop_sequence definition

-- Drop table

-- DROP TABLE carriermgmt.schedule_header_stop_sequence;

CREATE TABLE carriermgmt.schedule_header_stop_sequences_stg (
	schedule_id int8 NOT NULL,
	sequence_number int8 NOT NULL,
	stop_office_id int8 NOT NULL,
	stop_office_name varchar(50) NULL,
	distance_from_prev_stop float8 NULL,
	arrival_time timestamp null,
	departure_time timestamp null,
	transit_time int4 null,
	stay_duration int4 null,
	cut_of_time int4 null,
	office_location_latitude numeric(10, 2) NULL,
	office_location_longitude numeric(10, 2) NULL,
	division_id varchar(255) null,
	circle_id varchar(255) NULL,
	section_id varchar(255) NULL,
	section_beat_name varchar(255) NULL,
	station_code varchar(255) NULL,
	station_name varchar(255) NULL,
	created_by varchar(50) NOT NULL,
	created_date timestamp DEFAULT now() NOT NULL,
	approval_status bool null,
	approved_by varchar(50) null,
	approved_date timestamp null,
	CONSTRAINT schedule_header_stop_sequences_stg_pk PRIMARY KEY (schedule_id,stop_office_id,departure_time)
);