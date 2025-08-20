SELECT 
		t1.article_number ,
		DATE(t1.event_time) AS Date,
		TO_CHAR(t1.event_time, 'HH24:MI:SS') AS Time,
		t3.ooe_name AS Office,
		'99999999' AS OfficeID,
		t2.event_name AS Event
	FROM ips_event AS t1
INNER JOIN ips_event_master AS t2 ON t1.event_cd = t2.event_cd
INNER JOIN ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd
	
	WHERE t1.article_number = 'RW504596406IN'
	ORDER BY Date, Time;

select * from 
ips_event

 SELECT
    t1.article_number,

    t1.event_time AS event_date,

    t2.event_name AS event_type,

    '99999999' AS office_id,

    t3.ooe_name AS office_name,

    'ext_ips_ips_event' AS source_table,

    '' AS delivery_status,

    8 AS sort_order
FROM mis_db.ext_ips_ips_event AS t1
INNER JOIN mis_db.ext_ips_ips_event_master AS t2 ON t1.event_cd = t2.event_cd
INNER JOIN mis_db.dim_ips_ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd;




ips_event_master DDL:

CREATE TABLE ips_event_master (
	iem_id serial4 NOT NULL,
	event_cd int8 NULL,
	event_name varchar(100) NULL,
	mail_unit_type varchar(8) NULL,
	inb_otb varchar(4) NULL,
	CONSTRAINT ips_event_master_pkey PRIMARY KEY (iem_id)
);

ooe_master DDL:
CREATE TABLE ooe_master (
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