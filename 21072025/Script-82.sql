SELECT * FROM system.parts WHERE table = 'ext_bagmgmt_bag_close_content' AND active = 1;


CHECK TABLE ext_bagmgmt_bag_close_content;
select distinct(event_type) from mis_db.tracking_event_mv tem where event_type='Item Booked' article_number='EZ771735211IN''RA973131783IN'
select * from mis_db.ext_pdmanagement_article_event tem where article_number='EZ771735211IN''RA973131783IN'


SELECT
		cxcm.article_number, cxcm.article_type, 
		--formatDateTime(toDateTime(cxcm.booking_date), '%d%m%Y'),
		--formatDateTime(toDateTime(cxcm.booking_time), '%H%M%S'),
		formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS formatted_booking_date,
		 formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%M%S') AS formatted_booking_time,
		cxcm.booking_office_facility_id,
		cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
		cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
		cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
		cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
		te.event_date AS event_date,
		te.event_date AS event_time,
		te.event_type AS event_code,
		te.office_id AS event_office_facilty_id, 
		te.office_name AS office_name, 
		te.event_type AS event_description
	FROM mis_db.customer_xml_customer_mv AS cxcm
	INNER JOIN (
		SELECT *
		FROM mis_db.tracking_event_mv AS t1
		ANY INNER JOIN (
			SELECT article_number, max(event_date) AS max_event_date
			FROM mis_db.tracking_event_mv
			GROUP BY article_number
		) AS t2
		ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
	) te ON cxcm.article_number = te.article_number
	LEFT JOIN mis_db.customer_log AS el
		ON cxcm.bulk_customer_id = el.customer_id
	WHERE cxcm.bulk_customer_id = '1000002954'
		AND parseDateTimeBestEffort(cxcm.booking_date) < now()
		AND (el.generation_time IS NULL OR toDateTime(te.event_date) > toDateTime(el.generation_time))
		
		
		
		
		SELECT
		cxcm.article_number,
		cxcm.article_type,
		formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS formatted_booking_date,
		formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%S') AS formatted_booking_time,
		cxcm.booking_office_facility_id,
		--md.facility_id,
		cxcm.booking_office_name,
		cxcm.booking_pin,
		cxcm.sender_address_city,
		cxcm.destination_office_facility_id,
		--mdo.facility_id,
		cxcm.destination_office_name,
		cxcm.destination_pincode,
		cxcm.destination_city,
		cxcm.destination_country,
		cxcm.receiver_name,
		cxcm.invoice_no,
		cxcm.line_item,
		cxcm.weight_value,
		cxcm.tariff,
		cxcm.cod_amount,
		cxcm.booking_type,
		cxcm.contract_number,
		cxcm.reference,
		cxcm.bulk_customer_id,
		te.event_date AS event_date,
		te.event_date AS event_time,
		-- New CASE statement for EventCode
		CASE
			WHEN te.event_type = 'Item Return' THEN 'ITEM_RETURN'
			WHEN te.event_type = 'Item delivery' THEN 'ITEM_DELIVERY'
			WHEN te.event_type = 'Item Bagging' THEN 'ITEM_BAGGING'
			WHEN te.event_type = 'Item Return' THEN 'ITEM_RETURN'
			WHEN te.event_type = 'Item Onhold' THEN 'ITEM_ONHOLD'
			WHEN te.event_type = 'Item redirect' THEN 'ITEM_REDIRECT'
			-- Removed the duplicate 'Item Return' as it's already covered
		END AS EventCode,
		te.office_id AS event_office_facilty_id,
		te.office_name AS office_name,
		-- New CASE statement for EventDescription
--		CASE
--			WHEN te.event_type = 'Item Return' THEN 'Item Return' -- Assuming 'RC' also means 'Item Return' for description
--			WHEN te.event_type = 'Item delivery' THEN 'Item delivery'
--			WHEN te.event_type = 'Item Bagging' THEN 'Item Bagging'
--			WHEN te.event_type = 'Item Return' THEN 'Item Return'
--			WHEN te.event_type = 'Item Onhold' THEN 'Item Onhold'
--			WHEN te.event_type = 'Item redirect' THEN 'Item redirect'
			-- Removed the duplicate 'Item Return' as it's already covered
		--END 
	te.event_type	AS event_description
	--FROM mis_db.customer_xml_customer_mv AS cxcm
	FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
	INNER JOIN (
		SELECT *
		FROM mis_db.tracking_event_mv AS t1
		ANY INNER JOIN (
			SELECT article_number, max(event_date) AS max_event_date
			FROM mis_db.tracking_event_mv
			GROUP BY article_number
		) AS t2
		ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
	) te ON cxcm.article_number = te.article_number
	LEFT JOIN mis_db.customer_log AS el
		ON cxcm.bulk_customer_id = el.customer_id
		--LEFT JOIN mis_db.dim_mdm_office as md on md.office_id=cxcm.booking_office_facility_id 
	--	LEFT JOIN mis_db.dim_mdm_office as mdo on mdo.office_id=cxcm.destination_office_facility_id
		
	WHERE cxcm.bulk_customer_id = '1000002954'
		AND parseDateTimeBestEffort(cxcm.booking_date) < now()
		AND (el.generation_time IS NULL OR toDateTime(te.event_date) > toDateTime(el.generation_time))
		
		select * from mis_db.booking_dom_mv where article_number='EZ771854298IN'
		select * from mis_db.ext_bagmgmt_bag_event where article_number='EZ771854298IN'
		select * from mis_db.ext_bagmgmt_bag_open_content where article_number='EZ771854298IN'
		select * from mis_db.tracking_event_mv where article_number='EZ771854298IN'
		select * from mis_db.ext_bagmgmt_bag_event where article_number='EZ771854298IN'
		select * from mis_db.mv_raw_bagmgmt_bag_content_open where article_number='EZ771854298IN'
		select * from mis_db.dim_mdm_office where office_id='21250001'--BN21150000652
		select * from mis_db.ext_mdm_office_master where csi_facility_id='BN21150000652'
		
		select * from mis_db.ext_mailbkg_mailbooking_dom where csi_facility_id='BN21150000652'
		destination_office_id 
		md_office_id_bkg
		
		
		
		SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    kmd.md_updated_date AS booking_date,

    kmd.md_updated_date AS booking_time,

    kmd.md_office_id_bkg AS booking_office_facility_id,
    kom.csi_facility_id As cfi,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    kmd.destination_office_id AS destination_office_facility_id,
    kon.csi_facility_id as cfi1,
    

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
--INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id


WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);




--------
-- mis_db.customer_xml_customer_mv definition

CREATE TABLE mis_db.new_customer_xml_facility_customer_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

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

    `tariff` Decimal(10,
 2),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;
-- mis_db.mv_customer_xml_customer_mv source

CREATE MATERIALIZED VIEW mis_db.mv_customer_xml_customer_mv TO mis_db.customer_xml_customer_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` DateTime64(6),

    `booking_time` DateTime64(6),

    `booking_office_facility_id` Int64,

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

    `weight_value` Decimal(10,
 3),

    `tariff` Decimal(10,
 2),

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
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);
------------------------------



SELECT
		cxcm.article_number, cxcm.article_type, 
		--formatDateTime(toDateTime(cxcm.booking_date), '%d%m%Y'),
		--formatDateTime(toDateTime(cxcm.booking_time), '%H%M%S'),
		formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS formatted_booking_date,
		 formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%M%S') AS formatted_booking_time,
		cxcm.booking_office_facility_id,
--		 kom.csi_facility_id As booking_office_facility_id,
		cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, 
cxcm.destination_office_facility_id,
--		kon.csi_facility_id as destination_office_facility_id,
		cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
		cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
		cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
		te.event_date AS event_date,
		te.event_date AS event_time,
		te.event_type AS event_code,
		te.office_id AS event_office_facilty_id, 
		te.office_name AS office_name, 
		te.event_type AS event_description
	FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
	INNER JOIN (
		SELECT *
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
--		INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
--INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.destination_office_id = kon.office_id
	WHERE cxcm.bulk_customer_id = '6000015942'
		AND parseDateTimeBestEffort(cxcm.booking_date) < now()
		
		
		
		select * from mis_db.new_customer_xml_facility_customer_mv AS cxcm
		INNER JOIN (
		SELECT *
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
--		INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
--INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.destination_office_id = kon.office_id
	WHERE cxcm.bulk_customer_id = '6000015942'
		AND parseDateTimeBestEffort(cxcm.booking_date) < now()
		 
		
		FROM mis_db.customer_xml_customer_mv AS cxcm
	INNER JOIN (
		SELECT *
		FROM mis_db.tracking_event_mv AS t1
		ANY INNER JOIN (
			SELECT article_number, max(event_date) AS max_event_date
			FROM mis_db.tracking_event_mv
			GROUP BY article_number
		) AS t2
		ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
	) te ON cxcm.article_number = te.article_number
	LEFT JOIN mis_db.customer_log AS el
		ON cxcm.bulk_customer_id = el.customer_id
	WHERE cxcm.bulk_customer_id = '1000002954'
		AND parseDateTimeBestEffort(cxcm.booking_date) < now()
		AND (el.generation_time IS NULL OR toDateTime(te.event_date) > toDateTime(el.generation_time))
		
		
		
		
		SELECT
		cxcm.article_number, cxcm.article_type, 
		--formatDateTime(toDateTime(cxcm.booking_date), '%d%m%Y'),
		--formatDateTime(toDateTime(cxcm.booking_time), '%H%M%S'),
		formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS formatted_booking_date,
		 formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%M%S') AS formatted_booking_time,
		cxcm.booking_office_facility_id,
		cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
		cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
		cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
		cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
		te.event_date AS event_date,
		te.event_date AS event_time,
		te.event_code AS event_code,
		te.office_id AS event_office_facilty_id, 
		te.office_name AS office_name, 
		te.event_type AS event_description,
		te.delivery_status as non_delivery_reason
	FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
	INNER JOIN (
		SELECT *
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
		
		select * from mis_db.new_customer_tracking_event_mv where article_number='RM015543122IN'
		
		
		
		
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
		
		