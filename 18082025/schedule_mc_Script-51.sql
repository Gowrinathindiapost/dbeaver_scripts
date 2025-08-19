-- carriermgmt.schedule definition

-- Drop table

-- DROP TABLE carriermgmt.schedule;

CREATE TABLE carriermgmt.schedule_mc (
	schedule_id serial4 NOT NULL,
	schedule_name varchar(255) NOT NULL,
	schedule_type varchar(20) NOT NULL,
	bag_type _varchar NOT NULL,
	source_facility_id int8 NOT NULL,
	source_facility_name varchar(50) NOT NULL,
	destination_facility_id int8 NOT NULL,
	destination_facility_name varchar(50) NOT NULL,
	schedule_start_time varchar(20) NOT NULL,
	schedule_create_office_id int8 NOT NULL,
	schedule_valid_from timestamp NOT NULL,
	schedule_valid_to timestamp NOT NULL,
	transport_mode varchar(20) NOT NULL,
	schedule_running_days _varchar NOT NULL,
	schedule_status carriermgmt."schedule_status_enum" NOT NULL,
	created_by varchar(50) NOT NULL,
	created_date timestamp NOT NULL,
	updated_by varchar(50) NULL,
	updated_date timestamp NULL,
	deleted_by varchar(50) NULL,
	deleted_date timestamp NULL,
	CONSTRAINT schedule_mc_pkey PRIMARY KEY (schedule_id)
);

-- Permissions

ALTER TABLE carriermgmt.schedule OWNER TO carriermgmt_admin;
GRANT ALL ON TABLE carriermgmt.schedule TO carriermgmt_admin;
GRANT SELECT ON TABLE carriermgmt.schedule TO carriermgmt_ro;
GRANT INSERT, UPDATE, SELECT, DELETE ON TABLE carriermgmt.schedule TO carriermgmt_rw;

ALTER TABLE carriermgmt.schedule_mc ADD schedule_coverage varchar(255) NULL;
ALTER TABLE carriermgmt.schedule_mc ADD schedule_arrival_time varchar(20) NULL;
ALTER TABLE carriermgmt.schedule_mc ADD approval_status bool NULL;
ALTER TABLE carriermgmt.schedule_mc ADD approved_by varchar(50) NULL;
ALTER TABLE carriermgmt.schedule_mc ADD schedule_alias_name varchar(255) NULL;
ALTER TABLE carriermgmt.schedule_mc ADD approved_date timestamp NULL;
ALTER TABLE carriermgmt.schedule_mc ADD departure_cut_off_time varchar(255) NULL;
ALTER TABLE carriermgmt.schedule_mc ADD arrival_cut_off_time varchar(255) NULL;

-----11062025
-- carriermgmt.schedule_approval_stop_sequence definition

-- Drop table

-- DROP TABLE carriermgmt.schedule_approval_stop_sequence;

CREATE TABLE carriermgmt.schedule_stop_sequence_approval (
	request_number bigserial NOT NULL,
	requested_post_id int8 NOT NULL,
	requested_employee_id int8 NOT NULL,
	approval_post_id int8 NOT NULL,
	approval_employee_id int8 NOT NULL,
	schedule_id int8 NOT NULL,
next_stop_approval_post_id int8 NOT NULL,
	next_stop_approval_employee_id int8 NOT NULL,
	
	sequence_number int8 NOT NULL,
	stop_office_id int8 NOT NULL,
	circle_id varchar(255) not null,
	stop_office_name varchar(50) NOT NULL,
	next_stop_office_id int8 NOT NULL,
	next_circle_id varchar(255) not null,	
	next_stop_office_name varchar(50) NOT NULL,
	distance_to_next_stop float8 NOT NULL,
	distance_unit varchar(10) NOT NULL,
	transit_day int4 NOT NULL,
	transit_hour int2 NOT NULL,
	transit_minute int2 NOT NULL,
	stay_duration_day int8 NOT NULL,
	stay_duration_hour int2 NOT NULL,
	stay_duration_minute int2 NOT NULL,
	departure_time time NOT NULL,
	arrival_time time NOT NULL,
	
	created_by varchar(50) NOT NULL,
	created_date timestamp DEFAULT now() NOT NULL,
	updated_by varchar(50) NULL,
	updated_date timestamp NULL,
	approval_status bool NULL,
	approval_remarks varchar(50) NULL,
	reject_remarks varchar(50) NULL,
	next_stop_approval_status bool NULL,
	next_stop_approval_remarks varchar(50) NULL,
	next_stop_reject_remarks varchar(50) NULL,
	
	CONSTRAINT schedule_stop_sequence_approval_pk PRIMARY KEY (request_number)
);

-- Permissions

ALTER TABLE carriermgmt.schedule_approval_stop_sequence OWNER TO postgres;
GRANT ALL ON TABLE carriermgmt.schedule_approval_stop_sequence TO postgres;


Here are `INSERT` statements with dummy values for your `carriermgmt.schedule_stop_sequence_approval` table. These examples illustrate different approval states for various stop sequences.

---

### Dummy Data Inserts

```sql
-- Scenario 1: Stop Sequence 1 - Both offices Pending Approval
INSERT INTO carriermgmt.schedule_stop_sequence_approval (
    request_number, requested_post_id, requested_employee_id, approval_post_id, approval_employee_id, schedule_id,
    next_stop_approval_post_id, next_stop_approval_employee_id, sequence_number, stop_office_id, circle_id, stop_office_name,
    next_stop_office_id, next_circle_id, next_stop_office_name, distance_to_next_stop, distance_unit, transit_day,
    transit_hour, transit_minute, stay_duration_day, stay_duration_hour, stay_duration_minute, departure_time,
    arrival_time, created_by, created_date, updated_by, updated_date, approval_status, approval_remarks, reject_remarks,
    next_stop_approval_status, next_stop_approval_remarks, next_stop_reject_remarks
) VALUES (
    nextval('carriermgmt.schedule_stop_sequence_approval_request_number_seq'::regclass),
    101, 1001, -- Maker's Post ID, Employee ID
    0, 0, -- From Office: Approval pending (dummy/default 0s for pending)
    1539, -- schedule_id
    0, 0, -- Next Stop Office: Approval pending (dummy/default 0s for pending)
    1, -- sequence_number
    9000012, 'TN', 'Kanniyakumari Division', -- Stop Office
    9000013, 'TN', 'Nagercoil H.O', -- Next Stop Office
    10.0, 'KM', 0, 0, 10, 0, 0, 0, '10:00:00', '10:10:00',
    'maker_user_1', now(), NULL, NULL,
    false, NULL, NULL, -- From Office approval status: Pending (false)
    false, NULL, NULL  -- Next Stop Office approval status: Pending (false)
);

-- Scenario 2: Stop Sequence 2 - Both offices Approved
INSERT INTO carriermgmt.schedule_stop_sequence_approval (
    request_number, requested_post_id, requested_employee_id, approval_post_id, approval_employee_id, schedule_id,
    next_stop_approval_post_id, next_stop_approval_employee_id, sequence_number, stop_office_id, circle_id, stop_office_name,
    next_stop_office_id, next_circle_id, next_stop_office_name, distance_to_next_stop, distance_unit, transit_day,
    transit_hour, transit_minute, stay_duration_day, stay_duration_hour, stay_duration_minute, departure_time,
    arrival_time, created_by, created_date, updated_by, updated_date, approval_status, approval_remarks, reject_remarks,
    next_stop_approval_status, next_stop_approval_remarks, next_stop_reject_remarks
) VALUES (
    nextval('carriermgmt.schedule_stop_sequence_approval_request_number_seq'::regclass),
    101, 1001, -- Maker's Post ID, Employee ID
    201, 2001, -- From Office: Approved by these details
    1539, -- schedule_id
    301, 3001, -- Next Stop Office: Approved by these details
    5, -- sequence_number
    9000015, 'TN', 'Kanniyakumari S.O', -- Stop Office
    9000018, 'TN', 'Leepuram B.O', -- Next Stop Office
    20.0, 'KM', 0, 0, 30, 0, 0, 10, '11:36:00', '12:06:00',
    'maker_user_1', '2024-03-03 13:08:19.073', 'approver_user_2', now(),
    true, 'Approved by KSO Management', NULL, -- From Office approval status: Approved
    true, 'Capacity confirmed at LBO', NULL   -- Next Stop Office approval status: Approved
);

-- Scenario 3: Stop Sequence 3 - From Office Approved, Next Stop Office Rejected
INSERT INTO carriermgmt.schedule_stop_sequence_approval (
    request_number, requested_post_id, requested_employee_id, approval_post_id, approval_employee_id, schedule_id,
    next_stop_approval_post_id, next_stop_approval_employee_id, sequence_number, stop_office_id, circle_id, stop_office_name,
    next_stop_office_id, next_circle_id, next_stop_office_name, distance_to_next_stop, distance_unit, transit_day,
    transit_hour, transit_minute, stay_duration_day, stay_duration_hour, stay_duration_minute, departure_time,
    arrival_time, created_by, created_date, updated_by, updated_date, approval_status, approval_remarks, reject_remarks,
    next_stop_approval_status, next_stop_approval_remarks, next_stop_reject_remarks
) VALUES (
    nextval('carriermgmt.schedule_stop_sequence_approval_request_number_seq'::regclass),
    101, 1001, -- Maker's Post ID, Employee ID
    401, 4001, -- From Office: Approved by these details
    1539, -- schedule_id
    501, 5001, -- Next Stop Office: Rejected by these details
    4, -- sequence_number
    9000014, 'TN', 'Thuckalay H.O', -- Stop Office
    9000015, 'TN', 'Kanniyakumari S.O', -- Next Stop Office
    20.0, 'KM', 0, 0, 30, 0, 0, 6, '10:56:00', '11:26:00',
    'maker_user_1', '2024-03-03 13:08:19.073', 'approver_user_3', now(),
    true, 'Departure time verified', NULL, -- From Office approval status: Approved
    false, NULL, 'Cannot accommodate at destination, capacity issue at arrival time.' -- Next Stop Office approval status: Rejected
);

-- Scenario 4: Stop Sequence 4 - From Office Approved, Next Stop Office Pending
INSERT INTO carriermgmt.schedule_stop_sequence_approval (
    request_number, requested_post_id, requested_employee_id, approval_post_id, approval_employee_id, schedule_id,
    next_stop_approval_post_id, next_stop_approval_employee_id, sequence_number, stop_office_id, circle_id, stop_office_name,
    next_stop_office_id, next_circle_id, next_stop_office_name, distance_to_next_stop, distance_unit, transit_day,
    transit_hour, transit_minute, stay_duration_day, stay_duration_hour, stay_duration_minute, departure_time,
    arrival_time, created_by, created_date, updated_by, updated_date, approval_status, approval_remarks, reject_remarks,
    next_stop_approval_status, next_stop_approval_remarks, next_stop_reject_remarks
) VALUES (
    nextval('carriermgmt.schedule_stop_sequence_approval_request_number_seq'::regclass),
    101, 1001, -- Maker's Post ID, Employee ID
    601, 6001, -- From Office: Approved by these details
    1539, -- schedule_id
    0, 0, -- Next Stop Office: Pending
    3, -- sequence_number
    9000016, 'TN', 'Nagercoil Town S.O', -- Stop Office
    9000014, 'TN', 'Thuckalay H.O', -- Next Stop Office
    12.0, 'KM', 0, 0, 20, 0, 0, 5, '10:30:00', '10:50:00',
    'maker_user_1', '2024-03-03 13:08:19.073', 'approver_user_4', now(),
    true, 'Route confirmed from Nagercoil Town', NULL, -- From Office approval status: Approved
    false, NULL, NULL  -- Next Stop Office approval status: Pending
);

-- Scenario 5: Stop Sequence 5 - Both offices Approved (another full approval)
INSERT INTO carriermgmt.schedule_stop_sequence_approval (
    request_number, requested_post_id, requested_employee_id, approval_post_id, approval_employee_id, schedule_id,
    next_stop_approval_post_id, next_stop_approval_employee_id, sequence_number, stop_office_id, circle_id, stop_office_name,
    next_stop_office_id, next_circle_id, next_stop_office_name, distance_to_next_stop, distance_unit, transit_day,
    transit_hour, transit_minute, stay_duration_day, stay_duration_hour, stay_duration_minute, departure_time,
    arrival_time, created_by, created_date, updated_by, updated_date, approval_status, approval_remarks, reject_remarks,
    next_stop_approval_status, next_stop_approval_remarks, next_stop_reject_remarks
) VALUES (
    nextval('carriermgmt.schedule_stop_sequence_approval_request_number_seq'::regclass),
    101, 1001, -- Maker's Post ID, Employee ID
    701, 7001, -- From Office: Approved
    1539, -- schedule_id
    801, 8001, -- Next Stop Office: Approved
    2, -- sequence_number
    9000013, 'TN', 'Nagercoil H.O', -- Stop Office
    9000016, 'TN', 'Nagercoil Town S.O', -- Next Stop Office
    5.0, 'KM', 0, 0, 10, 0, 0, 5, '10:15:00', '10:25:00',
    'maker_user_1', '2024-03-03 13:08:19.073', 'approver_user_5', now(),
    true, 'Confirmed', NULL, -- From Office approval status: Approved
    true, 'Received and verified', NULL   -- Next Stop Office approval status: Approved
);
```

----12062025
ALTER TYPE carriermgmt."schedule_status_enum" ADD VALUE 'MODIFY';

