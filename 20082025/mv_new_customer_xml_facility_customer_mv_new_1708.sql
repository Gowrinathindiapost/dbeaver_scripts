--international not created
--------------------14
-- mis_db.mv_new_customer_xml_facility_customer_mv_new source


drop TABLE mis_db.mv_new_customer_xml_facility_customer_mv_new ON CLUSTER cluster_1S_2R;

CREATE MATERIALIZED VIEW mis_db.mv_new_customer_xml_facility_customer_mv_new TO mis_db.new_customer_xml_facility_customer_new_mv
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

    `tariff` Decimal(10,
 2),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64
)
AS SELECT DISTINCT
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
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON (kmd.sender_address_reference = kba1.address_id) AND (kmd.sender_pincode = kba1.pincode)
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON (kmd.receiver_address_reference = kba2.address_id) AND (kmd.receiver_pincode = kba2.pincode)
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.destination_office_id = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0) --AND (kmd.md_updated_date >= '2025-08-01');
AND (kmd.bulk_customer_id !=0)

----------------------13
-- mis_db.mv_new_customer_xml_facility_customer_mv source

drop TABLE mis_db.mv_new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R;

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

    `tariff` Decimal(10,
 2),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64
)
AS SELECT DISTINCT
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
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON (kmd.sender_address_reference = kba1.address_id) AND (kmd.sender_pincode = kba1.pincode)
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON (kmd.receiver_address_reference = kba2.address_id) AND (kmd.receiver_pincode = kba2.pincode)
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.destination_office_id = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0)
AND (kmd.bulk_customer_id !=0)
---------------------12
-- mis_db.mv_new_customer_xml_facility_customer_intl_mv_new source

drop TABLE mis_db.mv_new_customer_xml_facility_customer_intl_mv_new ON CLUSTER cluster_1S_2R;

CREATE MATERIALIZED VIEW mis_db.mv_new_customer_xml_facility_customer_intl_mv_new TO mis_db.new_customer_xml_facility_customer_new_mv
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

    `tariff` Decimal(10,
 2),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64
)
AS SELECT DISTINCT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    kmd.md_updated_date AS booking_date,

    kmd.md_updated_date AS booking_time,

    kom.csi_facility_id AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    '' AS destination_office_facility_id,

    '' AS destination_office_name,

    0 AS destination_pincode,

    '' AS destination_city,

    kmd.destination_cname AS destination_country,

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
FROM mis_db.ext_mailbkg_mailbooking_intl AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON (kmd.sender_address_reference = kba1.address_id) AND (kmd.sender_pincode = kba1.pincode)
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.md_office_id_bkg = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);

------------------11
-- mis_db.mv_new_customer_xml_facility_customer_intl_mv source

drop TABLE mis_db.mv_new_customer_xml_facility_customer_intl_mv ON CLUSTER cluster_1S_2R;

CREATE MATERIALIZED VIEW mis_db.mv_new_customer_xml_facility_customer_intl_mv TO mis_db.new_customer_xml_facility_customer_mv
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

    `tariff` Decimal(10,
 2),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64
)
AS SELECT DISTINCT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    kmd.md_updated_date AS booking_date,

    kmd.md_updated_date AS booking_time,

    kom.csi_facility_id AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    '' AS destination_office_facility_id,

    '' AS destination_office_name,

    0 AS destination_pincode,

    '' AS destination_city,

    kmd.destination_cname AS destination_country,

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
FROM mis_db.ext_mailbkg_mailbooking_intl AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON (kmd.sender_address_reference = kba1.address_id) AND (kmd.sender_pincode = kba1.pincode)
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.md_office_id_bkg = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);



--------------------4
-- mis_db.mv_ips_event_ooe_master_tracking_event source

drop TABLE mis_db.mv_ips_event_ooe_master_tracking_event ON CLUSTER cluster_1S_2R;

CREATE MATERIALIZED VIEW mis_db.mv_ips_event_ooe_master_tracking_event TO mis_db.tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS SELECT
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

-------------------3
-- mis_db.mv_ips_event_ooe_master_intl_tracking_event_new source

drop TABLE mis_db.mv_ips_event_ooe_master_intl_tracking_event_new ON CLUSTER cluster_1S_2R;


CREATE MATERIALIZED VIEW mis_db.mv_ips_event_ooe_master_intl_tracking_event_new TO mis_db.new_customer_tracking_event_new_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS SELECT
    t1.article_number,

    t1.event_time AS event_date,

    t2.event_name AS event_type,

    t2.event_code AS event_code,

    '99999999' AS office_id,

    t3.ooe_name AS office_name,

    'ext_ips_ips_event' AS source_table,

    '' AS delivery_status,

    8 AS sort_order
FROM mis_db.ext_ips_ips_event AS t1
INNER JOIN mis_db.ext_ips_ips_event_master AS t2 ON t1.event_cd = t2.event_cd
INNER JOIN mis_db.dim_ips_ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd;
-------------------2
-- mis_db.mv_ips_event_ooe_master_intl_tracking_event source

drop TABLE mis_db.mv_ips_event_ooe_master_intl_tracking_event ON CLUSTER cluster_1S_2R;

CREATE MATERIALIZED VIEW mis_db.mv_ips_event_ooe_master_intl_tracking_event TO mis_db.new_customer_tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS SELECT
    t1.article_number,

    t1.event_time AS event_date,

    t2.event_name AS event_type,

    t2.event_code AS event_code,

    '99999999' AS office_id,

    t3.ooe_name AS office_name,

    'ext_ips_ips_event' AS source_table,

    '' AS delivery_status,

    8 AS sort_order
FROM mis_db.ext_ips_ips_event AS t1
INNER JOIN mis_db.ext_ips_ips_event_master AS t2 ON t1.event_cd = t2.event_cd
INNER JOIN mis_db.dim_ips_ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd;
-----------------------------1
-- mis_db.amazon_target_table_dt_ib_mv source


drop TABLE mis_db.amazon_target_table_dt_ib_mv ON CLUSTER cluster_1S_2R;
--old query below 
CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_ib_mv TO mis_db.amazon_target_table_dt_ib
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

    `tariff` Decimal(10,
 2),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64,

    `event_date` DateTime64(6),

    `event_time` DateTime64(6),

    `event_code` String,

    `event_office_facilty_id` String,

    `office_name` String,

    `event_description` String,

    `non_delivery_reason` String
)
AS SELECT DISTINCT
    cxcm.article_number AS article_number,

    cxcm.article_type AS article_type,

    cxcm.booking_date AS booking_date,

    cxcm.booking_time AS booking_time,

    cxcm.booking_office_facility_id AS booking_office_facility_id,

    cxcm.booking_office_name AS booking_office_name,

    cxcm.booking_pin AS booking_pin,

    cxcm.sender_address_city AS sender_address_city,

    cxcm.destination_office_facility_id AS destination_office_facility_id,

    cxcm.destination_office_name AS destination_office_name,

    cxcm.destination_pincode AS destination_pincode,

    cxcm.destination_city AS destination_city,

    cxcm.destination_country AS destination_country,

    cxcm.receiver_name AS receiver_name,

    cxcm.invoice_no AS invoice_no,

    cxcm.line_item AS line_item,

    cxcm.weight_value AS weight_value,

    cxcm.tariff AS tariff,

    cxcm.cod_amount AS cod_amount,

    cxcm.booking_type AS booking_type,

    cxcm.contract_number AS contract_number,

    cxcm.reference AS reference,

    cxcm.bulk_customer_id AS bulk_customer_id,

    te.event_date AS event_date,

    toDateTime64(te.event_date,
 6) + toIntervalSecond(0) AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
LEFT JOIN
(
    SELECT
        article_number,

        event_date,

        event_code,

        office_id,

        office_name,

        event_type,

        delivery_status
    FROM mis_db.new_customer_tracking_event_new_mv
    --WHERE event_code = 'ITEM_BOOK'
) AS te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el ON cxcm.bulk_customer_id = el.customer_id
WHERE (cxcm.bulk_customer_id != 0) AND (te.article_number IS NOT NULL);
---new query below
CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_ib_mv TO mis_db.amazon_target_table_dt_ib
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

    `tariff` Decimal(10,
 2),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64,

    `event_date` DateTime64(6),

    `event_time` DateTime64(6),

    `event_code` String,

    `event_office_facilty_id` String,

    `office_name` String,

    `event_description` String,

    `non_delivery_reason` String
)
AS SELECT DISTINCT
    cxcm.article_number AS article_number,
    cxcm.article_type AS article_type,
    cxcm.booking_date AS booking_date,
    cxcm.booking_time AS booking_time,
    cxcm.booking_office_facility_id AS booking_office_facility_id,
    cxcm.booking_office_name AS booking_office_name,
    cxcm.booking_pin AS booking_pin,
    cxcm.sender_address_city AS sender_address_city,
    cxcm.destination_office_facility_id AS destination_office_facility_id,
    cxcm.destination_office_name AS destination_office_name,
    cxcm.destination_pincode AS destination_pincode,
    cxcm.destination_city AS destination_city,
    cxcm.destination_country AS destination_country,
    cxcm.receiver_name AS receiver_name,
    cxcm.invoice_no AS invoice_no,
    cxcm.line_item AS line_item,
    cxcm.weight_value AS weight_value,
    cxcm.tariff AS tariff,
    cxcm.cod_amount AS cod_amount,
    cxcm.booking_type AS booking_type,
    cxcm.contract_number AS contract_number,
    cxcm.reference AS reference,
    cxcm.bulk_customer_id AS bulk_customer_id,
    te.event_date AS event_date,
    toDateTime64(te.event_date, 6) + toIntervalSecond(0) AS event_time,
    te.event_code AS event_code,
    te.office_id AS event_office_facilty_id,
    te.office_name AS office_name,
    te.event_type AS event_description,
    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
INNER JOIN (
    SELECT
        article_number,
        event_date,
        event_code,
        office_id,
        office_name,
        event_type,
        delivery_status
    FROM mis_db.new_customer_tracking_event_new_mv
    WHERE event_code = 'ITEM_BOOK'
) AS te ON cxcm.article_number = te.article_number
--LEFT JOIN mis_db.customer_log AS el ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id != 0
