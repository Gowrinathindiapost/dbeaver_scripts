CREATE TABLE mis_db.customer_xml_customer_mv ON CLUSTER cluster_1S_2R
(
    `article_number` String,
    `article_type` String,
    `booking_date` String,  -- Changed from DateTime
    `booking_time` String,  -- Changed from DateTime
    `booking_office_facility_id` Int64, -- Use Int64 as per MV, or check source type
    `booking_office_name` String,
    `booking_pin` Int32,
    `sender_address_city` String,
    `destination_office_facility_id` Int32,
    `destination_office_name` String,
    `destination_pincode` Int32,
    `destination_city` String,
    `destination_country` String,
    `receiver_name` String,
    `invoice_no` String,
    `line_item` String,
    `weight_value` Decimal(10, 3),
    `tariff` Decimal(10, 2),
    `cod_amount` Decimal(10, 2), -- Changed from Float32
    `booking_type` String,
    `contract_number` Int32,  -- Changed from String
    `reference` String,
    `bulk_customer_id` Int64
    -- Event columns are still missing from your MV's SELECT.
    -- If you want them in the target table, you must add them to the MV's SELECT.
    -- Otherwise, remove them from the CREATE TABLE statement.
--    `event_code` String,
--    `event_description` String,
--    `event_office_facilty_id` Int32,
--    `event_office_name` String,
--    `event_date` DateTime,
--    `event_time` DateTime,
--    `non_del_reason` String
)
ENGINE = ReplicatedMergeTree
ORDER BY (article_number)
SETTINGS index_granularity = 8192;




CREATE MATERIALIZED VIEW mis_db.mv_customer_xml_customer_mv -- Renamed MV for clarity, or use your preferred MV name
ON CLUSTER cluster_1S_2R
TO mis_db.customer_xml_customer_mv -- <--- THIS MUST MATCH YOUR CREATE TABLE NAME
AS
INSERT INTO mis_db.customer_xml_customer_mv
SELECT
    kmd.article_number AS article_number,
    kmd.mail_type_code AS article_type,
    kmd.md_updated_date AS booking_date,
    kmd.md_updated_date AS booking_time,
    kmd.md_office_id_bkg AS booking_office_facility_id,
    kom.office_name AS booking_office_name,
    kmd.origin_pincode AS booking_pin,
    kba1.city AS sender_address_city,
    kmd.destination_office_id AS destination_office_facility_id,
    kmd.destination_office_name AS destination_office_name,
    kmd.destination_pincode AS destination_pincode,
    kba2.city AS destination_city,
    'INDIA' AS destination_country,
    kba2.addressee_name AS receiver_name,
   	kmd.bkg_ref_id AS invoice_no,
    '' AS line_item,
    kmd.charged_weight AS weight_value,
    kcd.total_amount AS tariff,
    kcd.vp_cod_value AS cod_amount,
    kmd.booking_type_code AS booking_type,
    kmd.contract_id AS contract_number,
    '' AS reference,
    kmd.bulk_customer_id AS bulk_customer_id
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1
    ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2
    ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom
    ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.status_code = 'PC' AND kmd._peerdb_is_deleted = 0;

--------------------------------------
CREATE TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R
(
    `article_number` String,
    `article_type` String,
    `booking_date` String,  -- Changed from DateTime
    `booking_time` String,  -- Changed from DateTime
    `booking_office_facility_id` String, -- Use Int64 as per MV, or check source type
    `booking_office_name` String,
    `booking_pin` Int32,
    `sender_address_city` String,
    `destination_office_facility_id` String,
    `destination_office_name` String,
    `destination_pincode` Int32,
    `destination_city` String,
    `destination_country` String,
    `receiver_name` String,
    `invoice_no` String,
    `line_item` String,
    `weight_value` Decimal(10, 3),
    `tariff` Decimal(10, 2),
    `cod_amount` Decimal(10, 2), -- Changed from Float32
    `booking_type` String,
    `contract_number` Int32,  -- Changed from String
    `reference` String,
    `bulk_customer_id` Int64
    -- Event columns are still missing from your MV's SELECT.
    -- If you want them in the target table, you must add them to the MV's SELECT.
    -- Otherwise, remove them from the CREATE TABLE statement.
--    `event_code` String,
--    `event_description` String,
--    `event_office_facilty_id` Int32,
--    `event_office_name` String,
--    `event_date` DateTime,
--    `event_time` DateTime,
--    `non_del_reason` String
)
ENGINE = ReplicatedMergeTree
ORDER BY (article_number)
SETTINGS index_granularity = 8192;




CREATE MATERIALIZED VIEW mis_db.mv_new_customer_xml_facility_customer_mv -- Renamed MV for clarity, or use your preferred MV name
ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_xml_facility_customer_mv -- <--- THIS MUST MATCH YOUR CREATE TABLE NAME
AS
INSERT INTO mis_db.new_customer_xml_facility_customer_mv
SELECT
    kmd.article_number AS article_number,
    kmd.mail_type_code AS article_type,
    kmd.md_updated_date AS booking_date,
    kmd.md_updated_date AS booking_time,
    kom.csi_facility_id AS booking_office_facility_id,
    kom.office_name AS booking_office_name,
    kmd.origin_pincode AS booking_pin,
    kba1.city AS sender_address_city,
    kon.csi_facility_id AS destination_office_facility_id,
    kmd.destination_office_name AS destination_office_name,
    kmd.destination_pincode AS destination_pincode,
    kba2.city AS destination_city,
    'INDIA' AS destination_country,
    kba2.addressee_name AS receiver_name,
   	kmd.bkg_ref_id AS invoice_no,
    '' AS line_item,
    kmd.charged_weight AS weight_value,
    kcd.total_amount AS tariff,
    kcd.vp_cod_value AS cod_amount,
    kmd.booking_type_code AS booking_type,
    kmd.contract_id AS contract_number,
    '' AS reference,
    kmd.bulk_customer_id AS bulk_customer_id
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1
    ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2
    ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext1_mdm_office_master AS kom
    ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext1_mdm_office_master AS kon 
ON kmd.destination_office_id = kon.office_id
WHERE kmd.status_code = 'PC' AND kmd._peerdb_is_deleted = 0;

select * FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd where status_code='PC'
select * from mis_db.ext_mailbkg_booking_address AS kba1

INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2
    ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom
    ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.status_code = 'PC' AND kmd._peerdb_is_deleted = 0;


-------------------
-- mis_db.ext_mailbkg_booking_address definition

CREATE TABLE mis_db.ext_mailbkg_booking_address
(

    `address_id` Int64,

    `addressee_name` String,

    `company_name` String,

    `city` String,

    `state` String,

    `pincode` Int32,

    `zip_code` String,

    `email_id` String,

    `alt_contact_no` String,

    `kyc_reference` String,

    `tax_reference` String,

    `mobile_no` Int64,

    `dac_id` String,

    `latitude` Decimal(10,
 6),

    `longitude` Decimal(10,
 6),

    `address_type_code` Int32,

    `md_created_date` DateTime64(6),

    `address_line_1` String,

    `address_line_2` String,

    `address_line_3` String,

    `booking_address_id` Int64,

    `_peerdb_synced_at` DateTime64(9) DEFAULT now64(),

    `_peerdb_is_deleted` Int8,

    `_peerdb_version` Int64
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PRIMARY KEY (address_id,
 address_type_code)
ORDER BY (address_id,
 address_type_code)
SETTINGS index_granularity = 8192;

---------------------------------------------
-- mis_db.ext_mdm_office_master definition
CREATE TABLE mis_db.customer_xml_customer_mv ON CLUSTER cluster_1S_2R
CREATE TABLE mis_db.ext_mdm_office_master ON CLUSTER cluster_1S_2R
(

    `office_id` Int32,

    `office_name` String,

    `office_type_id` Int32,

    `office_type_code` String,

    `email_id` String,

    `contact_number` String,

    `office_class` String,

    `pincode` Int32,

    `reporting_office_id` Int32,

    `office_status_id` Int32,

    `csi_facility_id` String,

    `closed_date` DateTime64(6),

    `supported_document_path` String,

    `admin_flag` Bool,

    `delivery_office_flag` Bool,

    `sol_id` String,

    `pli_id` String,

    `gstn_code` String,

    `pao_code` String,

    `atm_id` String,

    `qr_terminal_id` String,

    `weg_code` String,

    `ddo_code` String,

    `office_level` String,

    `dac` String,

    `working_days` String,

    `working_hours_from` String,

    `working_hours_to` String,

    `valid_from` Date32,

    `valid_to` Date32,

    `remarks` String,

    `approval_status` String,

    `approved_by` String,

    `created_by` String,

    `created_date` DateTime64(6),

    `updated_by` String,

    `updated_date` DateTime64(6),

    `approved_date` DateTime64(6),

    `modified_flag` Bool,

    `deleted_flag` Bool,

    `office_status` String,

    `accounting_office_id` Int32,

    `order_memo_number` String,

    `admin_office_id` Int32,

    `new_office_id` Int32,

    `facility_id` String,

    `state_code` Int32,

    `rollout_date` Date32,

    `rollout_confirmed` Bool,

    `_peerdb_synced_at` DateTime64(9) DEFAULT now64(),

    `_peerdb_is_deleted` Int8,

    `_peerdb_version` Int64,

    `head_of_the_office` Int32
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}',
 _peerdb_version)
PRIMARY KEY office_id
ORDER BY office_id
SETTINGS index_granularity = 8192;
ENGINE = ReplicatedMergeTree
ORDER BY (article_number)
SETTINGS index_granularity = 8192;

---------------------------------------------
-- mis_db.mv_new_customer_xml_facility_customer_mv source

CREATE MATERIALIZED VIEW mis_db.mv_new_customer_xml_facility_customer_mv TO mis_db.new_customer_xml_facility_customer_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` DateTime64(6),

    `booking_time` DateTime64(6),

    `booking_office_facility_id` String,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` String,

    `destination_office_name` String,

    `destination_pincode` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` Decimal(10,
 3),

    `tariff` Decimal(12,
 6),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64
)
AS SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    kmd.md_updated_date AS booking_date,

    kmd.md_updated_date AS booking_time,

    kom.csi_facility_id AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    kon.csi_facility_id AS destination_office_facility_id,

    kmd.destination_office_name AS destination_office_name,

    kmd.destination_pincode AS destination_pincode,

    kba2.city AS destination_city,

    'INDIA' AS destination_country,

    kba2.addressee_name AS receiver_name,

    kmd.bkg_ref_id AS invoice_no,

    '' AS line_item,

    kmd.charged_weight AS weight_value,

    kcd.total_amount AS tariff,

    kcd.vp_cod_value AS cod_amount,

    kmd.booking_type_code AS booking_type,

    kmd.contract_id AS contract_number,

    '' AS reference,

    kmd.bulk_customer_id AS bulk_customer_id
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.destination_office_id = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);


select * from mis_db.new_customer_xml_facility_customer_mv

--------------------
SELECT
		cxcm.article_number, cxcm.article_type, 
		formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
		 formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
		cxcm.booking_office_facility_id,
		cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
		cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
		cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
		cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
		formatDateTime((te.event_date), '%d%m%Y') AS event_date,
		 formatDateTime((te.event_date), '%H%i%s') AS event_time,
--		te.event_type AS event_code,
--		te.office_id AS event_office_facilty_id, 
--		te.office_name AS office_name, 
--		te.delivery_status AS event_description
		 te.event_code AS event_code,
		te.office_id AS event_office_facilty_id, 
		te.office_name AS office_name, 
		te.event_type AS event_description,
		te.delivery_status as non_delivery_reason
	FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
	INNER JOIN (
		SELECT t1.article_number, t1.event_date, t1.event_type, t1.event_code,t1.office_id, t1.office_name, t1.delivery_status
		FROM mis_db.new_customer_tracking_event_mv AS t1
		ANY INNER JOIN (
			SELECT article_number, max(event_date) AS max_event_date
			FROM mis_db.new_customer_tracking_event_mv
			GROUP BY article_number
		) AS t2
		ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
	) te ON cxcm.article_number = te.article_number
	LEFT JOIN mis_db.customer_log AS el
		ON cxcm.bulk_customer_id = el.customer_id
	WHERE cxcm.bulk_customer_id = '1000002954'
		AND parseDateTimeBestEffort(cxcm.booking_date) < now()
		AND (el.generation_time IS NULL OR toDateTime(te.event_date) > toDateTime(el.generation_time))
		
		INSERT INTO mis_db.new_customer_xml_facility_customer_mv (article_number, article_type, booking_date, booking_time, booking_office_facility_id, booking_office_name, booking_pin, sender_address_city, destination_office_facility_id, destination_office_name, destination_pincode, destination_city, destination_country, receiver_name, invoice_no, line_item, weight_value, tariff, cod_amount, booking_type, contract_number, reference, bulk_customer_id) 
VALUES('EK525406515IN', 'SP_INLAND', '2025-06-24 22:23:51.791618', '2025-06-24 22:23:51.791618', 21460007, 'Mysuru NSH', 570001, 'MYSURU', 22660999, 'Elathur SO Kozhikode', 673303, 'MYSURU', 'INDIA', 'SHAMA MYS', '2146000724062567614', '360.000', 60.00, 0, 0, 'WIC', 0, '', 1000002954);

EK525406589IN	SP_INLAND	2025-06-24 22:23:51.791618	2025-06-24 22:23:51.791618	21460007	Mysuru NSH	570001	MYSURU	22660999	Elathur SO Kozhikode	673303	MYSURU	INDIA	SHAMA MYS	2146000724062567614		360.000	60.00	0.00	WIC	0		1000002954

INSERT INTO mis_db.new_customer_tracking_event_mv (article_number, event_date, event_type, office_id, office_name, source_table, delivery_status, sort_order) 
VALUES('EK525406515IN', '2025-05-28 23:53:30', 'Item Bagged', 29460002, 'Chennai NSH', 'ext_bagmgmt_bag_event', 'testing', 0);