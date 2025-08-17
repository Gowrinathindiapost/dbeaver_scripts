kafka_article_recall_return
kafka_domestic_article_detail
kafka_pickup_main
kafka_office_master
kafka_charges_detail
kafka_induction_domestic
kafka_mailbooking_ord

---------------------------

-- trackandtrace.kafka_mailbooking_ord definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_mailbooking_ord;

CREATE TABLE trackandtrace.kafka_mailbooking_ord ( mailbooking_ord_id int8 DEFAULT 0 NOT NULL, origin_pincode int4 NOT NULL, destination_pincode int4 NOT NULL, physical_weight numeric(10, 3) NOT NULL, mail_shape_code varchar(20) NULL, dimension_length numeric(10, 3) NULL, dimension_breadth numeric(10, 3) NULL, dimension_height numeric(10, 3) NULL, volumetric_weight numeric(10, 3) NULL, charged_weight numeric(10, 3) NOT NULL, mail_type_code varchar(30) NOT NULL, receiver_pincode int4 NULL, article_number varchar(13) NOT NULL, md_updated_date timestamp NULL, destination_office_id int4 NULL, destination_office_name varchar(50) NULL, charges_detail_id int8 NOT NULL, md_office_id_bkg int8 NULL, CONSTRAINT kafka_mailbooking_ord_pkey2 PRIMARY KEY (mailbooking_ord_id));
CREATE INDEX idx_mailbooking_ord_charges_detail_id ON trackandtrace.kafka_mailbooking_ord USING btree (charges_detail_id);
CREATE INDEX kafka_mailbooking_ord_article_number_idx ON trackandtrace.kafka_mailbooking_ord USING btree (article_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_mailbooking_ord OWNER TO replication_group;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_mailbooking_ord TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_mailbooking_ord TO trackandtracerouser;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_mailbooking_ord TO trackandtracerwuser;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_mailbooking_ord TO debezium;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_mailbooking_ord TO yathish;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_mailbooking_ord TO sutheerdha;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_mailbooking_ord TO karthika;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_mailbooking_ord TO rohinitrackandtracerw;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_mailbooking_ord TO bharath;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_mailbooking_ord TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_mailbooking_ord TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_mailbooking_ord TO trackndtrace;


-- trackandtrace.kafka_induction_domestic definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_induction_domestic;

CREATE TABLE trackandtrace.kafka_induction_domestic ( dom_id int4 DEFAULT 0 NOT NULL, origin_pin int4 NULL, destination_pin int4 NULL, charged_weight numeric(10, 3) NULL, mail_service_type varchar(255) NULL, insurance_flag bool NULL, insurance_type varchar(255) NULL, article_number varchar(255) NULL, total_tariff numeric(10, 2) NULL, shift_no int4 NULL, induction_pos_date timestamp NULL, office_id_induction int4 NULL, CONSTRAINT kafka_induction_domestic_pkey PRIMARY KEY (dom_id));
CREATE INDEX kafka_induction_domestic_article_number_idx ON trackandtrace.kafka_induction_domestic USING btree (article_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_induction_domestic OWNER TO replication_group;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_induction_domestic TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_induction_domestic TO trackandtracerouser;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_induction_domestic TO trackandtracerwuser;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_induction_domestic TO debezium;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_induction_domestic TO yathish;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_induction_domestic TO sutheerdha;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_induction_domestic TO karthika;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_induction_domestic TO rohinitrackandtracerw;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_induction_domestic TO bharath;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_induction_domestic TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_induction_domestic TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_induction_domestic TO trackndtrace;

-- trackandtrace.kafka_charges_detail definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_charges_detail;

CREATE TABLE trackandtrace.kafka_charges_detail ( charges_detail_id int8 DEFAULT 0 NOT NULL, total_amount numeric(10, 2) NOT NULL, CONSTRAINT kafka_charges_detail_pkey1 PRIMARY KEY (charges_detail_id));

-- Permissions

ALTER TABLE trackandtrace.kafka_charges_detail OWNER TO replication_group;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_charges_detail TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_charges_detail TO trackandtracerouser;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_charges_detail TO trackandtracerwuser;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_charges_detail TO debezium;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_charges_detail TO yathish;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_charges_detail TO sutheerdha;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_charges_detail TO karthika;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_charges_detail TO rohinitrackandtracerw;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_charges_detail TO bharath;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_charges_detail TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_charges_detail TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_charges_detail TO trackndtrace;


-- trackandtrace.kafka_office_master definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_office_master;

CREATE TABLE trackandtrace.kafka_office_master ( office_id int4 NOT NULL, office_name varchar(50) NULL, office_type_id int4 NULL, office_type_code varchar(20) NULL, office_class varchar(50) NULL, pincode int4 NULL, reporting_office_id int4 NULL, office_status_id int4 NULL, csi_facility_id varchar(20) NULL, closed_date timestamp NULL, admin_flag bool NULL, delivery_office_flag bool NULL, sol_id varchar(20) NULL, pli_id varchar(20) NULL, pao_code varchar(20) NULL, ddo_code varchar(20) NULL, office_level varchar(20) NULL, working_days varchar(100) NULL, working_hours_from varchar(10) NULL, working_hours_to varchar(10) NULL, valid_from date NULL, valid_to date NULL, remarks varchar(200) NULL, CONSTRAINT kafka_office_master_pkey PRIMARY KEY (office_id));
CREATE INDEX idx_office_master_id ON trackandtrace.kafka_office_master USING btree (office_id);
CREATE INDEX idx_office_master_office_name ON trackandtrace.kafka_office_master USING btree (office_name);

-- Permissions

ALTER TABLE trackandtrace.kafka_office_master OWNER TO replication_group;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_office_master TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_office_master TO trackandtracerouser;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_office_master TO trackandtracerwuser;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_office_master TO debezium;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_office_master TO yathish;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_office_master TO sutheerdha;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_office_master TO karthika;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_office_master TO rohinitrackandtracerw;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_office_master TO bharath;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_office_master TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_office_master TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_office_master TO trackndtrace;

-- trackandtrace.kafka_pickup_main definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_pickup_main;

CREATE TABLE trackandtrace.kafka_pickup_main ( pickup_request_id int4 DEFAULT 0 NOT NULL, customer_id int8 NOT NULL, pickup_drop_type varchar(20) DEFAULT 'pickup'::text NULL, drop_location varchar(255) NULL, pickup_schedule_slot varchar(50) NOT NULL, pickup_schedule_date timestamp NOT NULL, actual_pickup_date timestamp NULL, pickup_agent_id int4 NULL, pickup_office_id int4 NULL, pickup_status varchar(30) DEFAULT 'Unassigned'::text NULL, payment_status varchar(40) NULL, created_date timestamp DEFAULT '1970-01-01 00:00:00'::timestamp without time zone NULL, pickup_address varchar(4000) NULL, domestic_foreign_identifier varchar(40) DEFAULT 'domestic'::text NULL, pickup_long numeric(25, 6) DEFAULT 0.000000 NULL, pickup_lat numeric(25, 6) DEFAULT 0.000000 NULL, updated_date timestamp DEFAULT '1970-01-01 00:00:00'::timestamp without time zone NULL, pickup_requested_pincode int4 NULL, customer_name varchar(100) NOT NULL, customer_mobile_number int8 NOT NULL, assigned_date timestamp NULL, remarks varchar(255) NULL, service varchar(255) NULL, package_description varchar(255) NULL, CONSTRAINT kafka_pickup_main_pkey PRIMARY KEY (pickup_request_id));
CREATE INDEX idx_pickup_main_pickup_office_id ON trackandtrace.kafka_pickup_main USING btree (pickup_office_id);
CREATE INDEX kafka_pickup_main_pickup_request_id_idx ON trackandtrace.kafka_pickup_main USING btree (pickup_request_id);

-- Permissions

ALTER TABLE trackandtrace.kafka_pickup_main OWNER TO replication_group;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_pickup_main TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_pickup_main TO trackandtracerouser;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_pickup_main TO trackandtracerwuser;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_pickup_main TO debezium;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_pickup_main TO yathish;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_pickup_main TO sutheerdha;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_pickup_main TO karthika;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_pickup_main TO rohinitrackandtracerw;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_pickup_main TO bharath;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_pickup_main TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_pickup_main TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_pickup_main TO trackndtrace;

-- trackandtrace.kafka_domestic_article_detail definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_domestic_article_detail;

CREATE TABLE trackandtrace.kafka_domestic_article_detail ( dom_id int4 DEFAULT 0 NOT NULL, pickup_request_id int4 NULL, article_number varchar(30) NOT NULL, article_state varchar(255) NULL, article_content varchar(255) NULL, article_image_id int4 NULL, is_premailing bool NULL, is_parcel_packing bool NULL, bkg_created_date timestamp NULL, bkg_updated_date timestamp DEFAULT '1900-01-01 01:01:01.001'::timestamp without time zone NULL, customer_dac_pickup varchar(255) NULL, address_type varchar(255) NULL, bkg_ref_id varchar(255) NULL, origin_pin int4 NOT NULL, destination_pin int4 NOT NULL, physical_weight numeric(10, 3) NOT NULL, shape varchar(255) NOT NULL, dimension_length numeric(10, 3) DEFAULT 0.000 NULL, dimension_breadth numeric(10, 3) DEFAULT 0.000 NULL, dimension_height numeric(10, 3) DEFAULT 0.000 NULL, volumetric_weight numeric(10, 3) NOT NULL, charged_weight numeric(10, 3) NULL, bkg_type varchar(255) NULL, mail_form varchar(255) NULL, is_prepaid bool NULL, prepayment_type varchar(255) NULL, value_of_prepayment numeric(10, 2) NULL, vp_cod_flag varchar NULL, insurance_flag bool NULL, insurance_type varchar(255) NULL, acknowledgement_pod bool NULL, instructions_rts varchar(255) NULL, address_ref_sender int8 NULL, address_ref_sender_alt_addr int8 NULL, pickup_flag bool NULL, base_tariff numeric(10, 2) NULL, tax numeric(10, 2) NULL, status varchar(255) NULL, bkg_created_by varchar(255) NULL, bkg_updated_by varchar(255) NULL, authorised_date timestamp NULL, authorised_by varchar(255) NULL, req_ip_address varchar(255) NULL, booking_channel varchar(255) NULL, customer_id int8 NULL, contract_id int4 NULL, is_parcel bool NULL, is_cod bool NULL, sender_name varchar(255) NOT NULL, sender_company_name varchar(255) NULL, sender_addrline1 varchar(255) NULL, sender_addrline2 varchar(255) NULL, sender_addrline3 varchar(255) NULL, sender_city varchar(255) NOT NULL, sender_state varchar(255) NOT NULL, sender_pincode int4 NOT NULL, sender_email_id varchar(255) NULL, sender_alt_contact_no varchar(30) NULL, sender_kyc_reference varchar(255) NULL, sender_tax_reference varchar(255) NULL, receiver_name varchar(255) NOT NULL, receiver_company_name varchar(255) NULL, receiver_addrline1 varchar(255) NULL, receiver_addrline2 varchar(255) NULL, receiver_addrline3 varchar(255) NULL, receiver_city varchar(255) NOT NULL, receiver_state varchar(255) NOT NULL, receiver_pincode int4 NOT NULL, receiver_email_id varchar(255) NULL, receiver_alt_contact_no varchar(30) NULL, receiver_kyc_reference varchar(255) NULL, receiver_tax_reference varchar(255) NULL, premailing_service varchar(255) NULL, box_or_nobox varchar(50) NULL, gst numeric(10, 2) NULL, service_charges numeric(10, 2) NULL, premailing_charges numeric(10, 2) NULL, instructions_delivery varchar(20) NULL, receiver_dac_id varchar(20) NULL, bkg_updated_ip_address varchar(255) NULL, destination_office_id int4 NULL, destination_office_name varchar(50) NULL, origin_office_name varchar(50) NULL, pickup_office_pincode int4 NULL, pickup_office_name varchar(50) NULL, sender_mobile_no int8 NULL, receiver_mobile_no int8 NULL, kafka_booking_id int8 NULL, kafka_charges_id int8 NULL, kafka_pickupaddress_id int8 NULL, priority_flag bool NULL, channel_type_cd varchar(20) NULL, CONSTRAINT kafka_domestic_article_detail_pkey PRIMARY KEY (dom_id));
CREATE INDEX idx_domestic_article_detail_pickup_request_id ON trackandtrace.kafka_domestic_article_detail USING btree (pickup_request_id);
CREATE INDEX kafka_domestic_article_detail_article_number_idx ON trackandtrace.kafka_domestic_article_detail USING btree (article_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_domestic_article_detail OWNER TO replication_group;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_domestic_article_detail TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_domestic_article_detail TO trackandtracerouser;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_domestic_article_detail TO trackandtracerwuser;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_domestic_article_detail TO debezium;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_domestic_article_detail TO yathish;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_domestic_article_detail TO sutheerdha;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_domestic_article_detail TO karthika;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_domestic_article_detail TO rohinitrackandtracerw;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_domestic_article_detail TO bharath;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_domestic_article_detail TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_domestic_article_detail TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_domestic_article_detail TO trackndtrace;


-- trackandtrace.kafka_article_recall_return definition

-- Drop table

-- DROP TABLE trackandtrace.kafka_article_recall_return;

CREATE TABLE trackandtrace.kafka_article_recall_return ( article_recall_return_id int8 DEFAULT 0 NOT NULL, article_number varchar(13) NOT NULL, booking_date timestamp DEFAULT '0001-01-03 00:00:00'::timestamp without time zone NULL, sender_pincode int4 NOT NULL, receiver_pincode int4 NOT NULL, transaction_date timestamp DEFAULT '0001-01-03 00:00:00'::timestamp without time zone NULL, is_recall bool DEFAULT false NULL, is_return bool DEFAULT false NULL, CONSTRAINT kafka_article_recall_return_pkey PRIMARY KEY (article_recall_return_id));
CREATE INDEX kafka_article_recall_return_article_number_idx ON trackandtrace.kafka_article_recall_return USING btree (article_number);

-- Permissions

ALTER TABLE trackandtrace.kafka_article_recall_return OWNER TO replication_group;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_article_recall_return TO replication_group;
GRANT SELECT ON TABLE trackandtrace.kafka_article_recall_return TO trackandtracerouser;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_article_recall_return TO trackandtracerwuser;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_article_recall_return TO debezium;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_article_recall_return TO yathish;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_article_recall_return TO sutheerdha;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_article_recall_return TO karthika;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE trackandtrace.kafka_article_recall_return TO rohinitrackandtracerw;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_article_recall_return TO bharath;
GRANT DELETE, TRUNCATE, UPDATE, REFERENCES, INSERT, SELECT, TRIGGER ON TABLE trackandtrace.kafka_article_recall_return TO ramesh;
GRANT SELECT ON TABLE trackandtrace.kafka_article_recall_return TO trackandtracebackupuser;
GRANT SELECT ON TABLE trackandtrace.kafka_article_recall_return TO trackndtrace;