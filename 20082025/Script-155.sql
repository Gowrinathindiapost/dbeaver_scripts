

ips_event_master DDL:

CREATE TABLE trackandtrace.ips_event_master (
	iem_id serial4 NOT NULL,
	event_cd int8 NULL,
	event_name varchar(100) NULL,
	mail_unit_type varchar(8) NULL,
	inb_otb varchar(4) NULL,
	CONSTRAINT ips_event_master_pkey PRIMARY KEY (iem_id)
);

ooe_master DDL:
CREATE TABLE trackandtrace.ooe_master (
	ooe_id serial4 NOT NULL,
	ooe_code int4 NULL,
	ooe_fcd varchar(50) NULL,
	fpo_code varchar(50) NULL,
	country_code varchar(2) NULL,
	ooe_name varchar(50) NULL,
	pincode int4 NULL,
	CONSTRAINT ooe_master_ooe_code_key UNIQUE (ooe_code),
	CONSTRAINT ooe_master_pkey PRIMARY KEY (ooe_id)
);


CREATE TABLE trackandtrace.article_event ( article_number varchar(13) NULL, office_id int4 NULL, event_date timestamp NULL, event_code varchar(10) NULL, entry_date timestamp NULL);