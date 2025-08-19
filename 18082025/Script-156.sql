-- ips.ips_event definition

-- Drop table

-- DROP TABLE ips.ips_event;

CREATE TABLE trackandtrace.kafka_ips_event ( event_pid uuid NOT NULL, article_pid uuid NULL, event_time timestamp NOT NULL, 
captured_time timestamp NOT NULL, 
event_cd int4 NOT NULL, office_cd varchar(50) NULL, user_cd varchar(50) NULL, remarks_cd varchar(100) NULL, next_office_cd varchar(50) NULL, ips_push bool NULL, bag_id varchar(35) NULL, 
non_delivery_reason_cd int4 NULL, non_delivery_measure_cd int4 NULL, is_ips bool NULL, mailbooking_intl_id int8 NULL, customs_duty numeric(10, 3) NULL, article_number varchar(13) NULL,
sender_id varchar(50) NULL, event_updated_by varchar(10) NULL, CONSTRAINT ips_event_pkey PRIMARY KEY (event_pid), CONSTRAINT ips_event_unique UNIQUE (mailbooking_intl_id));

