-- trackandtrace.kafka_mailbooking_intl definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_mailbooking_intl;

CREATE TABLE trackandtrace.kafka_mailbooking_intl ( mailbooking_intl_id int8 NOT NULL, origin_pincode int4 NULL, origin_country_code varchar(15) NULL, destination_ccode varchar(15) NOT NULL, destination_cname varchar(50) NOT NULL, mail_type_code varchar(30) NULL, charged_weight numeric(10, 3) NOT NULL, article_number varchar(13) NOT NULL, md_office_id_bkg int8 NULL, md_updated_date timestamp NULL, charges_detail_id int8 NULL, sender_address_reference int8 NULL, sender_pincode int4 NULL, sender_mobile_no int8 NULL, receiver_address_reference int8 NULL, CONSTRAINT kafka_mailbooking_intl_pkey PRIMARY KEY (mailbooking_intl_id));
CREATE INDEX idx_mailbooking_intl_charges_detail_id ON trackandtrace.kafka_mailbooking_intl USING btree (charges_detail_id);
CREATE INDEX kafka_mailbooking_intl_article_number_idx ON trackandtrace.kafka_mailbooking_intl USING btree (article_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_mailbooking_intl OWNER TO replication_group;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_intl TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_mailbooking_intl TO trackandtracerouser;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE trackandtrace.kafka_mailbooking_intl TO trackandtracerwuser;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_intl TO debezium;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_intl TO yathish;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_intl TO sutheerdha;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_intl TO karthika;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE trackandtrace.kafka_mailbooking_intl TO rohinitrackandtracerw;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_intl TO bharath;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_intl TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_mailbooking_intl TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_mailbooking_intl TO trackndtrace;


-- trackandtrace.kafka_mailbooking_dom definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_mailbooking_dom;

CREATE TABLE trackandtrace.kafka_mailbooking_dom ( mailbooking_dom_id int8 NOT NULL, origin_pincode int4 NOT NULL, destination_pincode int4 NOT NULL, physical_weight numeric(10, 3) NOT NULL, charged_weight numeric(10, 3) NOT NULL, mail_type_code varchar(30) NOT NULL, priority_flag bool NULL, instructions_delivery varchar(20) NULL, delivery_slot varchar(20) NULL, instructions_rts varchar(20) NULL, sender_pincode int4 NULL, pickup_flag bool NULL, article_number varchar(13) NOT NULL, payment_mode_code varchar(15) NOT NULL, bkg_ref_id varchar(50) NOT NULL, status_code varchar(30) NOT NULL, md_counter_no int4 NOT NULL, md_shift_no int4 NOT NULL, md_created_date timestamp NOT NULL, md_created_by varchar(20) NOT NULL, md_office_id_bkg int8 NOT NULL, md_ip_address_bkg varchar(50) NOT NULL, md_updated_date timestamp NULL, destination_office_id int4 NULL, destination_office_name varchar(50) NULL, origin_office_name varchar(50) NULL, pickup_office_pincode int4 NULL, pickup_office_id int4 NULL, pickup_office_name varchar(50) NULL, charges_detail_id int8 NOT NULL, pickup_schedule_slot varchar(30) NULL, pickup_schedule_date timestamp NULL, sender_address_reference int8 NULL, sender_mobile_no int8 NULL, receiver_address_reference int8 NULL, bulk_customer_id int8 NULL, CONSTRAINT kafka_mailbooking_dom_pkey2 PRIMARY KEY (mailbooking_dom_id));
CREATE INDEX idx_mailbooking_dom_charges_detail_id ON trackandtrace.kafka_mailbooking_dom USING btree (charges_detail_id);
CREATE INDEX kafka_mailbooking_dom_article_number_idx ON trackandtrace.kafka_mailbooking_dom USING btree (article_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_mailbooking_dom OWNER TO replication_group;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_dom TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_mailbooking_dom TO trackandtracerouser;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE trackandtrace.kafka_mailbooking_dom TO trackandtracerwuser;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_dom TO debezium;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_dom TO yathish;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_dom TO sutheerdha;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_dom TO karthika;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE trackandtrace.kafka_mailbooking_dom TO rohinitrackandtracerw;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_dom TO bharath;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_mailbooking_dom TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_mailbooking_dom TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_mailbooking_dom TO trackndtrace;


-- trackandtrace.kafka_bag_event definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_bag_event;

CREATE TABLE trackandtrace.kafka_bag_event ( bag_event_id int4 DEFAULT 0 NOT NULL, bag_number varchar(29) NULL, transaction_date timestamp NULL, bag_type varchar(3) NULL, from_office_id int4 NULL, to_office_id int4 NULL, event_type varchar(2) NULL, transaction_date_time varchar NULL, CONSTRAINT kafka_bag_event_pkey PRIMARY KEY (bag_event_id));
CREATE INDEX idx_bag_event_article_event ON trackandtrace.kafka_bag_event USING btree (bag_number, event_type);
CREATE INDEX idx_bag_event_from_office ON trackandtrace.kafka_bag_event USING btree (from_office_id);
CREATE INDEX idx_bag_event_to_office ON trackandtrace.kafka_bag_event USING btree (to_office_id);
CREATE INDEX kafka_bag_event_bag_number_idx ON trackandtrace.kafka_bag_event USING btree (bag_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_bag_event OWNER TO replication_group;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_event TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_bag_event TO trackandtracerouser;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE trackandtrace.kafka_bag_event TO trackandtracerwuser;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_event TO debezium;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_event TO yathish;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_event TO sutheerdha;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_event TO karthika;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE trackandtrace.kafka_bag_event TO rohinitrackandtracerw;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_event TO bharath;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_event TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_bag_event TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_bag_event TO trackndtrace;


-- trackandtrace.kafka_bag_close_content definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_bag_close_content;

CREATE TABLE trackandtrace.kafka_bag_close_content ( bag_number varchar(29) NOT NULL, article_number varchar(13) NULL, article_type varchar(50) NULL, art_status varchar(1) NULL, insured_flag bool NULL, bag_close_content_id int4 DEFAULT 0 NOT NULL, CONSTRAINT kafka_bag_close_content_pkey PRIMARY KEY (bag_close_content_id));
CREATE INDEX idx_bag_close_article_bag ON trackandtrace.kafka_bag_close_content USING btree (article_number, bag_number);
CREATE INDEX idx_bag_close_article_event_type ON trackandtrace.kafka_bag_close_content USING btree (bag_number, article_number);
CREATE INDEX kafka_bag_close_content_article_number_idx ON trackandtrace.kafka_bag_close_content USING btree (article_number);
CREATE INDEX kafka_bag_close_content_bag_number_idx ON trackandtrace.kafka_bag_close_content USING btree (bag_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_bag_close_content OWNER TO replication_group;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_close_content TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_bag_close_content TO trackandtracerouser;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE trackandtrace.kafka_bag_close_content TO trackandtracerwuser;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_close_content TO debezium;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_close_content TO yathish;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_close_content TO sutheerdha;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_close_content TO karthika;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE trackandtrace.kafka_bag_close_content TO rohinitrackandtracerw;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_close_content TO bharath;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_close_content TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_bag_close_content TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_bag_close_content TO trackndtrace;

-- trackandtrace.kafka_bag_open_content definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_bag_open_content;

CREATE TABLE trackandtrace.kafka_bag_open_content ( bag_number varchar(29) NULL, article_number varchar(13) NULL, article_type varchar(50) NULL, art_status varchar(1) NULL, insured_flag bool NULL, bag_open_content_id int4 DEFAULT 0 NOT NULL, CONSTRAINT kafka_bag_open_content_pkey PRIMARY KEY (bag_open_content_id));
CREATE INDEX idx_bag_open_article_number ON trackandtrace.kafka_bag_open_content USING btree (article_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_bag_open_content OWNER TO replication_group;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_open_content TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_bag_open_content TO trackandtracerouser;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE trackandtrace.kafka_bag_open_content TO trackandtracerwuser;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_open_content TO debezium;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_open_content TO yathish;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_open_content TO sutheerdha;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_open_content TO karthika;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE trackandtrace.kafka_bag_open_content TO rohinitrackandtracerw;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_open_content TO bharath;
GRANT REFERENCES, TRIGGER, SELECT, UPDATE, INSERT, TRUNCATE, DELETE ON TABLE trackandtrace.kafka_bag_open_content TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_bag_open_content TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_bag_open_content TO trackndtrace;


-- trackandtrace.kafka_article_event definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_article_event;

CREATE TABLE trackandtrace.kafka_article_event ( article_event_id int8 DEFAULT 0 NOT NULL, article_number varchar(13) NOT NULL, event_code varchar(30) NOT NULL, event_date timestamp DEFAULT '0001-01-03'::date NULL, remarks varchar(100) DEFAULT ''::text NULL, created_by varchar(100) NOT NULL, current_office_pincode int4 DEFAULT 0 NULL, current_office_id int4 DEFAULT 0 NULL, current_office_name varchar(50) DEFAULT ''::text NULL, redirected_office_pincode int4 DEFAULT 0 NULL, redirected_office_id int4 DEFAULT 0 NULL, redirected_office_name varchar(50) DEFAULT ''::text NULL, beat_name varchar(30) DEFAULT ''::text NULL, postman_name varchar(100) DEFAULT ''::text NULL, CONSTRAINT kafka_article_event_pkey PRIMARY KEY (article_event_id));
CREATE INDEX idx_article_event_article_number ON trackandtrace.kafka_article_event USING btree (article_number);
CREATE INDEX idx_article_event_current_office_id ON trackandtrace.kafka_article_event USING btree (current_office_id);
CREATE INDEX idx_article_event_event_code ON trackandtrace.kafka_article_event USING btree (event_code);
CREATE INDEX idx_article_event_id_filtered ON trackandtrace.kafka_article_event USING btree (article_number) WHERE ((event_code)::text = 'ID'::text);
CREATE INDEX idx_article_event_number_code ON trackandtrace.kafka_article_event USING btree (article_number, event_code);

-- Permissions

ALTER TABLE trackandtrace.kafka_article_event OWNER TO replication_group;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_event TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_article_event TO trackandtracerouser;
GRANT INSERT, DELETE, UPDATE, SELECT ON TABLE trackandtrace.kafka_article_event TO trackandtracerwuser;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_event TO debezium;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_event TO yathish;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_event TO sutheerdha;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_event TO karthika;
GRANT INSERT, DELETE, UPDATE, SELECT ON TABLE trackandtrace.kafka_article_event TO rohinitrackandtracerw;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_event TO bharath;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_event TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_article_event TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_article_event TO trackndtrace;



-- trackandtrace.kafka_article_transaction definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_article_transaction;

CREATE TABLE trackandtrace.kafka_article_transaction ( article_transaction_id int8 DEFAULT 0 NOT NULL, article_number varchar(13) NOT NULL, booking_date timestamp DEFAULT '0001-01-03 00:00:00'::timestamp without time zone NULL, article_received_date timestamp DEFAULT '0001-01-03 00:00:00'::timestamp without time zone NULL, article_type varchar(250) DEFAULT ''::text NULL, is_vpp bool DEFAULT false NULL, is_cod bool DEFAULT false NULL, vpp_value numeric(10, 2) DEFAULT 0.00 NULL, cod_value numeric(10, 2) DEFAULT 0.00 NULL, vpp_commission numeric(10, 2) DEFAULT 0.00 NULL, customs_duty numeric(10, 2) DEFAULT 0.00 NULL, source_pincode int4 DEFAULT 0 NULL, source_office_id int4 NOT NULL, source_office_name varchar(50) DEFAULT ''::text NULL, destination_pincode int4 DEFAULT 0 NULL, destination_office_id int4 NOT NULL, destination_office_name varchar(50) DEFAULT ''::text NULL, redirected_pincode int4 DEFAULT 0 NULL, redirected_office_id int4 DEFAULT 0 NULL, redirected_office_name varchar(50) DEFAULT ''::text NULL, sender_address1 varchar(100) DEFAULT ''::text NULL, sender_address2 varchar(100) DEFAULT ''::text NULL, sender_address3 varchar(100) DEFAULT ''::text NULL, sender_city varchar(50) DEFAULT ''::text NULL, sender_state varchar(50) DEFAULT ''::text NULL, receiver_address1 varchar(100) DEFAULT ''::text NULL, receiver_address2 varchar(100) DEFAULT ''::text NULL, receiver_address3 varchar(100) DEFAULT ''::text NULL, receiver_city varchar(50) DEFAULT ''::text NULL, receiver_state varchar(50) DEFAULT ''::text NULL, sender_name varchar(50) NOT NULL, receiver_name varchar(100) NOT NULL, bag_number varchar(50) DEFAULT ''::text NULL, is_bulk_article bool DEFAULT false NULL, bulk_addressee_id varchar(20) DEFAULT ''::text NULL, bulk_addressee_name varchar(100) DEFAULT ''::text NULL, is_received bool DEFAULT false NULL, batch_name varchar(15) DEFAULT ''::text NULL, beat_name varchar(20) DEFAULT ''::text NULL, personnel_id int4 DEFAULT 0 NULL, personnel_name varchar(100) DEFAULT ''::text NULL, is_bo_article bool DEFAULT false NULL, bo_id int4 DEFAULT 0 NULL, bo_name varchar(50) DEFAULT ''::text NULL, redirected_bo_id int4 DEFAULT 0 NULL, redirected_bo_name varchar(50) DEFAULT ''::text NULL, is_invoiced bool DEFAULT false NULL, invoiced_date timestamp DEFAULT '0001-01-03 00:00:00'::timestamp without time zone NULL, is_delivered bool DEFAULT false NULL, delivered_date timestamp DEFAULT '0001-01-03 00:00:00'::timestamp without time zone NULL, remarks_id varchar(10) DEFAULT ''::text NULL, remarks varchar(255) DEFAULT ''::text NULL, action_code int4 DEFAULT 0 NULL, action_to_be_taken varchar(100) DEFAULT ''::text NULL, is_returned bool DEFAULT false NULL, is_redirected bool DEFAULT false NULL, is_recalled bool DEFAULT false NULL, id_proof_document_type varchar(100) DEFAULT ''::text NULL, id_proof_document_number varchar(100) DEFAULT ''::text NULL, created_by varchar(100) NOT NULL, created_date timestamp DEFAULT '0001-01-03 00:00:00'::timestamp without time zone NULL, supervisor_approval_type int4 DEFAULT 0 NULL, supervisor_approval_flag bool DEFAULT false NULL, is_deposited bool DEFAULT false NULL, prod_id int4 DEFAULT 0 NULL, sender_pincode int4 DEFAULT 0 NULL, current_office_pincode int4 DEFAULT 0 NULL, current_office_id int4 NOT NULL, current_office_name varchar(50) DEFAULT ''::text NULL, CONSTRAINT kafka_article_transaction_pkey PRIMARY KEY (article_transaction_id));
CREATE INDEX idx_article_transaction_action_code ON trackandtrace.kafka_article_transaction USING btree (action_code);
CREATE INDEX kafka_article_transaction_article_number_idx ON trackandtrace.kafka_article_transaction USING btree (article_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_article_transaction OWNER TO replication_group;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_transaction TO replication_group;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_transaction TO yathish;
GRANT SELECT ON TABLE trackandtrace.kafka_article_transaction TO trackandtracerouser;
GRANT INSERT, DELETE, UPDATE, SELECT ON TABLE trackandtrace.kafka_article_transaction TO trackandtracerwuser;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_transaction TO debezium;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_transaction TO sutheerdha;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_transaction TO karthika;
GRANT INSERT, DELETE, UPDATE, SELECT ON TABLE trackandtrace.kafka_article_transaction TO rohinitrackandtracerw;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_transaction TO bharath;
GRANT REFERENCES, TRIGGER, INSERT, DELETE, UPDATE, TRUNCATE, SELECT ON TABLE trackandtrace.kafka_article_transaction TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_article_transaction TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_article_transaction TO trackndtrace;











