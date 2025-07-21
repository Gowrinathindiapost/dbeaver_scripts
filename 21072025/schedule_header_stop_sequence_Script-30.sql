
CREATE TABLE carriermgmt.schedule_header_stop_sequence (
	schedule_id int8 NOT NULL,
	sequence_number int8 NOT NULL,--stop_id
	stop_office_id int8 NULL,
	stop_office_name varchar(50) NULL,
	distance_from_prev_stop float8 NULL,
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
	office_location_latitude numeric(10,2),  
    office_location_longitude numeric(10,2),
   
    
    
	CONSTRAINT schedule_header_stop_sequence_pk PRIMARY KEY (schedule_id,stop_office_id,departure_time)
);

 CREATE TABLE carriermgmt.schedule_approval_stop_sequence (
 	request_number bigserial not null,
 	requested_post_id int8 not null,
 	requested_employee_id int8 not null,
 	approval_post_id int8 not null,
 	approval_employee_id int8 not null,
 	schedule_id int8 NOT NULL,
 	schedule_name varchar(255) not null,
 	created_by varchar(50) NOT NULL,
	created_date timestamp DEFAULT now() NOT NULL,
    updated_by varchar(50) null,
    updated_date timestamp null,
    approval_status bool null,
    approval_remarks varchar(50) null,
    reject_remarks varchar(50) null,
    reconcile_status bool null,
	CONSTRAINT schedule_approval_stop_sequence_pk PRIMARY KEY (request_number)
);

StopsData : sr no, seq no, stop id, stop name , arrival time Date , departure time  date, transit time Second, stay duration second , lat, log 
RequestsofSchedule requ no, requed post id, req emp id, reqsted to post id, requ emp id, schedule id, sch name, date, changes. apprroved false approved date, approved remark rejected remark


sr no, seq no, stop id, stop name , arrival time Date , departure time  date, transit time Second, stay duration second , lat, log , circle id, station code , station name , section beat name
{ 
schedule _id
shceule_name
arrival time
last stop /destination name
destination id
stop_data:[{
sr no, seq no, stop id, stop name , arrival time Date , departure time  date, transit time Second, stay duration second , lat, log , circle id, station code , station name , section beat name
}}
