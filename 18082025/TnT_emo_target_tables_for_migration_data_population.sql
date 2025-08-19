-- trackandtrace.kafka_mobooking_dom definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_mobooking_dom;

CREATE TABLE trackandtrace.kafka_mobooking_dom ( mobooking_dom_id int4 DEFAULT 0 NOT NULL, origin_pin int4 NOT NULL, origin_name varchar(80) NOT NULL, origin_office_id int4 NOT NULL, destination_pin int4 NOT NULL, destination_office_id int4 NOT NULL, destination_name varchar(80) NOT NULL, mo_type_code varchar(6) NOT NULL, mo_value numeric(9, 2) NOT NULL, mo_commission numeric(9, 2) NOT NULL, total_value numeric(9, 2) NOT NULL, sender_name varchar(80) NOT NULL, sender_company_name varchar(80) NULL, sender_address_line1 varchar(80) NOT NULL, receiver_name varchar(80) NOT NULL, receiver_company_name varchar(80) NULL, receiver_address_line1 varchar(80) NOT NULL, receiver_city varchar(80) NOT NULL, receiver_state varchar(80) NULL, receiver_pincode int4 NULL, pnr_no varchar(80) NOT NULL, receiver_country varchar(80) NULL, md_updated_date timestamp NULL, CONSTRAINT kafka_mobooking_dom_pkey PRIMARY KEY (mobooking_dom_id));
CREATE INDEX kafka_mobooking_dom_pnr_no_idx ON trackandtrace.kafka_mobooking_dom USING btree (pnr_no);

-- Permissions

ALTER TABLE trackandtrace.kafka_mobooking_dom OWNER TO replication_group;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_dom TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_mobooking_dom TO trackandtracerouser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE trackandtrace.kafka_mobooking_dom TO trackandtracerwuser;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_dom TO debezium;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_dom TO yathish;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_dom TO sutheerdha;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_dom TO karthika;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE trackandtrace.kafka_mobooking_dom TO rohinitrackandtracerw;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_dom TO bharath;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_dom TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_mobooking_dom TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_mobooking_dom TO trackndtrace;



-- trackandtrace.kafka_mobooking_ap definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_mobooking_ap;

CREATE TABLE trackandtrace.kafka_mobooking_ap ( mobooking_aps_id int8 DEFAULT 0 NOT NULL, origin_pin int4 NOT NULL, origin_name varchar(80) NOT NULL, origin_office_id int4 NOT NULL, destination_pin int8 NOT NULL, destination_office_id int4 NOT NULL, destination_name varchar(80) NOT NULL, mo_type_code varchar(6) NOT NULL, mo_value numeric(9, 2) NOT NULL, mo_commission numeric(9, 2) NOT NULL, total_value numeric(9, 2) NOT NULL, sender_name varchar(80) NOT NULL, sender_company_name varchar(80) NULL, sender_address_line1 varchar(80) NOT NULL, receiver_name varchar(80) NOT NULL, receiver_company_name varchar(80) NULL, receiver_address_line1 varchar(80) NOT NULL, receiver_city varchar(80) NOT NULL, receiver_state varchar(80) NULL, receiver_pincode int4 NULL, pnr_no varchar NOT NULL, md_updated_date timestamp NULL, receiver_country varchar(80) NULL, CONSTRAINT kafka_mobooking_ap_pkey PRIMARY KEY (mobooking_aps_id));
CREATE INDEX kafka_mobooking_ap_pnr_no_idx ON trackandtrace.kafka_mobooking_ap USING btree (pnr_no);

-- Permissions

ALTER TABLE trackandtrace.kafka_mobooking_ap OWNER TO replication_group;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_ap TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_mobooking_ap TO trackandtracerouser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE trackandtrace.kafka_mobooking_ap TO trackandtracerwuser;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_ap TO debezium;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_ap TO yathish;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_ap TO sutheerdha;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_ap TO karthika;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE trackandtrace.kafka_mobooking_ap TO rohinitrackandtracerw;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_ap TO bharath;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_mobooking_ap TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_mobooking_ap TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_mobooking_ap TO trackndtrace;


-- trackandtrace.kafka_emo_event definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_emo_event;

CREATE TABLE trackandtrace.kafka_emo_event ( emo_id int8 DEFAULT 0 NOT NULL, emo_number varchar(20) NOT NULL, emo_value numeric(8, 2) NOT NULL, source_pincode int4 NOT NULL, source_office_id int4 NOT NULL, source_office_name varchar(50) NOT NULL, destination_pincode int4 NOT NULL, destination_office_id int4 NOT NULL, destination_office_name varchar(50) NOT NULL, redirected_pincode int4 DEFAULT 0 NULL, redirected_office_id int4 DEFAULT 0 NULL, redirected_office_name varchar(50) DEFAULT ''::text NULL, current_office_pincode int4 NOT NULL, current_office_id int4 NOT NULL, current_office_name varchar(50) NOT NULL, event_date timestamp DEFAULT '0001-01-03 00:00:00'::timestamp without time zone NULL, event_code varchar(10) NOT NULL, remarks varchar(100) DEFAULT ''::text NULL, created_by varchar(100) DEFAULT '0'::text NULL, CONSTRAINT kafka_emo_event_pkey PRIMARY KEY (emo_id));
CREATE INDEX kafka_emo_event_current_office_id_idx ON trackandtrace.kafka_emo_event USING btree (current_office_id);
CREATE INDEX kafka_emo_event_emo_number_idx ON trackandtrace.kafka_emo_event USING btree (emo_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_emo_event OWNER TO replication_group;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_emo_event TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_emo_event TO trackandtracerouser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE trackandtrace.kafka_emo_event TO trackandtracerwuser;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_emo_event TO debezium;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_emo_event TO yathish;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_emo_event TO sutheerdha;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_emo_event TO karthika;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE trackandtrace.kafka_emo_event TO rohinitrackandtracerw;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_emo_event TO bharath;
GRANT SELECT, INSERT, UPDATE, REFERENCES, TRIGGER, DELETE, TRUNCATE ON TABLE trackandtrace.kafka_emo_event TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_emo_event TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_emo_event TO trackndtrace;