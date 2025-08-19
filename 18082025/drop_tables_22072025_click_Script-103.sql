drop table mis_db.xml_customer_mv ON CLUSTER cluster_1S_2R
drop table mis_db.xml_PC_test_mv ON CLUSTER cluster_1S_2R

drop table mis_db.mailbooking_dom_customer_PC_mv ON CLUSTER cluster_1S_2R
drop table mis_db.mailbooking_dom_xml_customer_PC_mv ON CLUSTER cluster_1S_2R
drop table mis_db.mailbooking_dom_customer_xml_mv ON CLUSTER cluster_1S_2R

drop table mis_db.mailbooking_dom_customer_xml_PC_test_mv ON CLUSTER cluster_1S_2R
drop table mis_db.mailbooking_dom_customer_xml_PC_mv ON CLUSTER cluster_1S_2R

drop table mis_db.mv_xml_customer_mv ON CLUSTER cluster_1S_2R
drop table mis_db.mv_xml_PC_test_mv ON CLUSTER cluster_1S_2R
drop table mis_db.mv_mailbooking_dom_xml_customer_PC_mv ON CLUSTER cluster_1S_2R
drop table mis_db.mv_mailbooking_dom_customer_xml_test_mv ON CLUSTER cluster_1S_2R

drop table mis_db.mv_mailbooking_dom_customer_xml_mv ON CLUSTER cluster_1S_2R
drop table mis_db.mv_mailbooking_dom_customer_xml_PC_test_mv  ON CLUSTER cluster_1S_2R
drop table mis_db.mv_mailbooking_dom_customer_xml_PC_mv  ON CLUSTER cluster_1S_2R
drop table mis_db.mv_mailbooking_dom_customer_PC_mv  ON CLUSTER cluster_1S_2R
drop table mis_db.mv_customer_xml_PC_test_mv  ON CLUSTER cluster_1S_2R
drop table mis_db.customer_xml_PC_test_mv  ON CLUSTER cluster_1S_2R
drop table mis_db.amazon_target_table_mv  ON CLUSTER cluster_1S_2R
drop table mis_db.amazon_target_table_ib_mv  ON CLUSTER cluster_1S_2R
drop table mis_db.mv_new_customer_xml_facility_customer_mv_new  ON CLUSTER cluster_1S_2R
drop table mis_db.new_customer_tracking_event_mv  ON CLUSTER cluster_1S_2R
drop table mis_db.new_customer_xml_facility_customer_mv  ON CLUSTER cluster_1S_2R

drop table mis_db.mv_new_article_events_tracking_event  ON CLUSTER cluster_1S_2R
drop table mis_db.mv_new_bag_events_tracking_event  ON CLUSTER cluster_1S_2R
drop table mis_db.mv_new_customer_xml_facility_customer_mv  ON CLUSTER cluster_1S_2R

drop table mis_db.mv_new_pdmanagement_article_transaction_tracking_event  ON CLUSTER cluster_1S_2R




SHOW CREATE TABLE mis_db.new_customer_xml_facility_customer_new_mv
SHOW CREATE TABLE mis_db.ext_mailbkg_mailbooking_dom;

-----------------------26
-- mis_db.mv_new_pdmanagement_article_transaction_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_new_pdmanagement_article_transaction_tracking_event TO mis_db.new_customer_tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime,

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS SELECT
    article_number,

    now() AS event_date,

    multiIf(action_code = 1,
 'Item Delivery',
 'Not Delivered') AS event_type,

    multiIf(action_code = 1,
 'ITEM_DELIVERY',
 'ITEM_NONDELIVER') AS event_code,

    csi_facility_id AS office_id,

    current_office_name AS office_name,

    'ext_pdmanagement_article_transaction' AS source_table,

    multiIf(action_code = 1,
 '',
 epat.remarks) AS delivery_status,

    7 AS sort_order
FROM mis_db.ext_pdmanagement_article_transaction AS epat
INNER JOIN mis_db.ext_mdm_office_master AS kom ON epat.current_office_id = kom.office_id
WHERE article_number IS NOT NULL;

------------------------------25
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

    CAST(kmd.md_updated_date,
 'String') AS booking_date,

    CAST(kmd.md_updated_date,
 'String') AS booking_time,

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
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0) AND (kmd.md_updated_date > toDateTime('2025-07-01 00:00:00'));

---------------------24
-- mis_db.mv_new_bag_events_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_new_bag_events_tracking_event TO mis_db.new_customer_tracking_event_mv
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
    coalesce(bcc.article_number,
 boc.article_number) AS article_number,

    be.transaction_date AS event_date,

    multiIf(be.event_type = 'CL',
 'Bag Close',
 be.event_type = 'DI',
 'Bag Dispatch',
 be.event_type = 'RO',
 'Item Received',
 be.event_type IN ('OP',
 'OR'),
 'Bag Open',
 '') AS event_type,

    multiIf(be.event_type = 'CL',
 'BAG_CLOSE',
 be.event_type = 'DI',
 'BAG_DISPATCH',
 be.event_type = 'RO',
 'TMO_RECEIVE',
 be.event_type IN ('OP',
 'OR'),
 'BAG_OPEN',
 '') AS event_code,

    coalesce(kom.csi_facility_id,
 kom2.csi_facility_id) AS office_id,

    coalesce(kom.office_name,
 kom2.office_name) AS office_name,

    'ext_bagmgmt_bag_event' AS source_table,

    '' AS delivery_status,

    3 AS sort_order
FROM mis_db.ext_bagmgmt_bag_event AS be
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content AS bcc ON (be.bag_number = bcc.bag_number) AND (be.event_type IN ('CL',
 'DI'))
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content AS boc ON (be.bag_number = boc.bag_number) AND (be.event_type IN ('OP',
 'OR'))
LEFT JOIN mis_db.ext_mdm_office_master AS kom ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 ON be.to_office_id = kom2.office_id
WHERE be.transaction_date > toDateTime('2025-07-01 00:00:00');

---------------------------------23
-- mis_db.mv_new_article_events_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_new_article_events_tracking_event TO mis_db.new_customer_tracking_event_mv
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
    kae.article_number,

    kae.event_date,

    multiIf(kae.event_code = 'RC',
 'Item Return',
 kae.event_code = 'ID',
 'Item delivery',
 kae.event_code = 'IN',
 'Item Bagging',
 kae.event_code = 'RT',
 'Item Return',
 kae.event_code = 'DE',
 'Item Onhold',
 kae.event_code = 'RD',
 'Item redirect',
 kae.event_code = 'IT',
 'Item Return',
 '') AS event_type,

    multiIf(kae.event_code = 'RC',
 'ITEM_RETURN',
 kae.event_code = 'ID',
 'ITEM_DELIVERY',
 kae.event_code = 'IN',
 'ITEM_BAGGING',
 kae.event_code = 'RT',
 'ITEM_RETURN',
 kae.event_code = 'DE',
 'ITEM_ONHOLD',
 kae.event_code = 'RD',
 'ITEM_REDIRECT',
 kae.event_code = 'IT',
 'ITEM_RETURN',
 '') AS event_code,

    kom.csi_facility_id AS office_id,

    kom.office_name,

    'ext_pdmanagement_article_event' AS source_table,

    '' AS delivery_status,

    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event AS kae
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kae.current_office_id = kom.office_id
WHERE kae.event_date > toDateTime('2025-07-01 00:00:00');
--------------------------------22
-- mis_db.new_customer_xml_facility_customer_mv definition

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

    `bulk_customer_id` Int64,

    `booking_date_parsed` DateTime MATERIALIZED parseDateTimeBestEffort(booking_date),

    INDEX idx_cxcm_customer_date (bulk_customer_id,
 parseDateTimeBestEffort(booking_date)) TYPE minmax GRANULARITY 3,

    INDEX idx_cxcm_article article_number TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;
---------------------------21
SET max_table_size_to_drop = 80000000000; -- 80 GB

DROP TABLE mis_db.new_customer_tracking_event_mv;


-- mis_db.new_customer_tracking_event_mv definition

CREATE TABLE mis_db.new_customer_tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime,

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8,

    INDEX idx_te_date_code (event_date,
 event_code) TYPE minmax GRANULARITY 3,

    INDEX idx_te_article_date_code (article_number,
 event_date,
 event_code) TYPE minmax GRANULARITY 3,

    INDEX idx_te_date event_date TYPE minmax GRANULARITY 3,

    INDEX idx_te_article_date (article_number,
 event_date) TYPE minmax GRANULARITY 3
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;
--------------------------20
-- mis_db.mv_new_customer_xml_facility_customer_mv_new source

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

    CAST(kmd.md_updated_date,
 'String') AS booking_date,

    CAST(kmd.md_updated_date,
 'String') AS booking_time,

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
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);


----------------------------19
-- mis_db.amazon_target_table_ib_mv source

CREATE MATERIALIZED VIEW mis_db.amazon_target_table_ib_mv TO mis_db.amazon_target_table_ib
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

    `bulk_customer_id` Int64,

    `event_date` String,

    `event_time` String,

    `event_code` String,

    `event_office_facilty_id` String,

    `office_name` String,

    `event_description` String,

    `non_delivery_reason` String
)
AS SELECT
    cxcm.article_number AS article_number,

    cxcm.article_type AS article_type,

    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date),
 '%d%m%Y') AS booking_date,

    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time),
 '%H%i%s') AS booking_time,

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

    formatDateTime(te.event_date,
 '%d%m%Y') AS event_date,

    formatDateTime(te.event_date,
 '%H%i%s') AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
INNER JOIN mis_db.new_customer_tracking_event_mv AS te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el ON cxcm.bulk_customer_id = el.customer_id
WHERE (cxcm.bulk_customer_id != 0) AND (te.event_code = 'ITEM_BOOK');

------------------------------18
-- mis_db.amazon_target_table_mv source

CREATE MATERIALIZED VIEW mis_db.amazon_target_table_mv TO mis_db.amazon_target_table
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

    `bulk_customer_id` Int64,

    `event_date` String,

    `event_time` String,

    `event_code` String,

    `event_office_facilty_id` String,

    `office_name` String,

    `event_description` String,

    `non_delivery_reason` String
)
AS SELECT
    cxcm.article_number,

    cxcm.article_type,

    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date),
 '%d%m%Y') AS booking_date,

    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time),
 '%H%i%s') AS booking_time,

    cxcm.booking_office_facility_id,

    cxcm.booking_office_name,

    cxcm.booking_pin,

    cxcm.sender_address_city,

    cxcm.destination_office_facility_id,

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

    formatDateTime(te.event_date,
 '%d%m%Y') AS event_date,

    formatDateTime(te.event_date,
 '%H%i%s') AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
INNER JOIN
(
    SELECT
        t1.article_number,

        t1.event_date,

        t1.event_type,

        t1.event_code,

        t1.office_id,

        t1.office_name,

        t1.delivery_status
    FROM mis_db.new_customer_tracking_event_mv AS t1
    ANY INNER JOIN
    (
        SELECT
            article_number,

            max(event_date) AS max_event_date
        FROM mis_db.new_customer_tracking_event_mv
        GROUP BY article_number
    ) AS t2 ON (t1.article_number = t2.article_number) AND (t1.event_date = t2.max_event_date)
) AS te ON cxcm.article_number = te.article_number
WHERE (cxcm.bulk_customer_id != 0) AND (parseDateTimeBestEffort(cxcm.booking_date) < now());
-------------------------17
-- mis_db.customer_xml_PC_test_mv definition

CREATE TABLE mis_db.customer_xml_PC_test_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

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

    `reference` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;


--------------------16
-- mis_db.mv_customer_xml_PC_test_mv source

CREATE MATERIALIZED VIEW mis_db.mv_customer_xml_PC_test_mv TO mis_db.customer_xml_PC_test_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

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

    `reference` String
)
AS SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    formatDateTime(kmd.md_created_date,
 '%d%m%Y') AS booking_date,

    formatDateTime(kmd.md_created_date,
 '%H%i%s') AS booking_time,

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

    '' AS reference
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);

----------------------------------------15
-- mis_db.mv_mailbooking_dom_customer_PC_mv source

CREATE MATERIALIZED VIEW mis_db.mv_mailbooking_dom_customer_PC_mv TO mis_db.mailbooking_dom_customer_PC_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

    `booking_office_facility_id` Int64,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

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

    `reference` String
)
AS SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    formatDateTime(kmd.md_created_date,
 '%d%m%Y') AS booking_date,

    formatDateTime(kmd.md_created_date,
 '%H%i%s') AS booking_time,

    kmd.md_office_id_bkg AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    kmd.destination_office_id AS destination_office_facility_id,

    kmd.destination_office_name AS destination_office_name,

    kmd.destination_pincode AS destination_pin,

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

    '' AS reference
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);

------------------------------------14
-- mis_db.mv_mailbooking_dom_customer_xml_PC_mv source

CREATE MATERIALIZED VIEW mis_db.mv_mailbooking_dom_customer_xml_PC_mv TO mis_db.mailbooking_dom_customer_xml_PC_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

    `booking_office_facility_id` Int64,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

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

    `reference` String
)
AS SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    formatDateTime(kmd.md_created_date,
 '%d%m%Y') AS booking_date,

    formatDateTime(kmd.md_created_date,
 '%H%i%s') AS booking_time,

    kmd.md_office_id_bkg AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    kmd.destination_office_id AS destination_office_facility_id,

    kmd.destination_office_name AS destination_office_name,

    kmd.destination_pincode AS destination_pin,

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

    '' AS reference
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);

---------------------------------------------------13
-- mis_db.mv_mailbooking_dom_customer_xml_PC_test_mv source

CREATE MATERIALIZED VIEW mis_db.mv_mailbooking_dom_customer_xml_PC_test_mv TO mis_db.mailbooking_dom_customer_xml_PC_test_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

    `booking_office_facility_id` Int64,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

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

    `reference` String
)
AS SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    formatDateTime(kmd.md_created_date,
 '%d%m%Y') AS booking_date,

    formatDateTime(kmd.md_created_date,
 '%H%i%s') AS booking_time,

    kmd.md_office_id_bkg AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    kmd.destination_office_id AS destination_office_facility_id,

    kmd.destination_office_name AS destination_office_name,

    kmd.destination_pincode AS destination_pin,

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

    '' AS reference
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);
----------------------------------12
-- mis_db.mv_mailbooking_dom_customer_xml_mv source

CREATE MATERIALIZED VIEW mis_db.mv_mailbooking_dom_customer_xml_mv TO mis_db.mailbooking_dom_customer_xml_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

    `booking_office_facility_id` Int64,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` Decimal(10,
 3),

    `tariff` Decimal(10,
 2),

    `cod_amount` Float64,

    `booking_type` String,

    `contract_number` String,

    `reference` String
)
AS SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    formatDateTime(kmd.md_created_date,
 '%d%m%Y') AS booking_date,

    formatDateTime(kmd.md_created_date,
 '%H%i%s') AS booking_time,

    kmd.md_office_id_bkg AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    kmd.destination_office_id AS destination_office_facility_id,

    kmd.destination_office_name AS destination_office_name,

    kmd.destination_pincode AS destination_pin,

    kba2.city AS destination_city,

    'INDIA' AS destination_country,

    kba2.addressee_name AS receiver_name,

    '' AS invoice_no,

    '' AS line_item,

    kmd.charged_weight AS weight_value,

    kcd.total_amount AS tariff,

    0. AS cod_amount,

    '' AS booking_type,

    '' AS contract_number,

    '' AS reference
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
LEFT JOIN mis_db.ext_pdmanagement_article_event AS kat ON (kmd.article_number = kat.article_number) AND (kat.event_code = 'ID')
WHERE kmd._peerdb_is_deleted = 0;

-----------------------------11

-- mis_db.mv_mailbooking_dom_customer_xml_test_mv source

CREATE MATERIALIZED VIEW mis_db.mv_mailbooking_dom_customer_xml_test_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

    `booking_office_facility_id` Int64,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` Decimal(10,
 3),

    `tariff` Decimal(10,
 2),

    `cod_amount` Float64,

    `booking_type` String,

    `contract_number` String,

    `reference` String
)
ENGINE = ReplacingMergeTree
ORDER BY article_number
SETTINGS index_granularity = 8192
AS SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    formatDateTime(kmd.md_created_date,
 '%d%m%Y') AS booking_date,

    formatDateTime(kmd.md_created_date,
 '%H%i%s') AS booking_time,

    kmd.md_office_id_bkg AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    kmd.destination_office_id AS destination_office_facility_id,

    kmd.destination_office_name AS destination_office_name,

    kmd.destination_pincode AS destination_pin,

    kba2.city AS destination_city,

    'INDIA' AS destination_country,

    kba2.addressee_name AS receiver_name,

    '' AS invoice_no,

    '' AS line_item,

    kmd.charged_weight AS weight_value,

    kcd.total_amount AS tariff,

    0. AS cod_amount,

    '' AS booking_type,

    '' AS contract_number,

    '' AS reference
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id;


--------------------------------------10

-- mis_db.mv_mailbooking_dom_xml_customer_PC_mv source

CREATE MATERIALIZED VIEW mis_db.mv_mailbooking_dom_xml_customer_PC_mv TO mis_db.mailbooking_dom_xml_customer_PC_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

    `booking_office_facility_id` Int64,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

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

    `reference` String
)
AS SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    formatDateTime(kmd.md_created_date,
 '%d%m%Y') AS booking_date,

    formatDateTime(kmd.md_created_date,
 '%H%i%s') AS booking_time,

    kmd.md_office_id_bkg AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    kmd.destination_office_id AS destination_office_facility_id,

    kmd.destination_office_name AS destination_office_name,

    kmd.destination_pincode AS destination_pin,

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

    '' AS reference
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);
-------------------------------------9

-- mis_db.mv_xml_PC_test_mv source

CREATE MATERIALIZED VIEW mis_db.mv_xml_PC_test_mv TO mis_db.xml_PC_test_mv
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

    `reference` String
)
AS SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    kmd.md_created_date AS booking_date,

    kmd.md_created_date AS booking_time,

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

    '' AS reference
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);


----------------------------------------8

-- mis_db.mv_xml_customer_mv source

CREATE MATERIALIZED VIEW mis_db.mv_xml_customer_mv TO mis_db.xml_customer_mv
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

    `reference` String
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

    '' AS reference
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);

---------------------------------------7

-- mis_db.mailbooking_dom_customer_xml_PC_mv definition

CREATE TABLE mis_db.mailbooking_dom_customer_xml_PC_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` DateTime,

    `booking_time` DateTime,

    `booking_office_facility_id` Int32,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` String,

    `tariff` Decimal(10,
 2),

    `cod_amount` Float32,

    `booking_type` String,

    `contract_number` String,

    `reference` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;
----------------------------------6

-- mis_db.mailbooking_dom_customer_xml_PC_test_mv definition

CREATE TABLE mis_db.mailbooking_dom_customer_xml_PC_test_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` DateTime,

    `booking_time` DateTime,

    `booking_office_facility_id` Int32,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` String,

    `tariff` Decimal(10,
 2),

    `cod_amount` Float32,

    `booking_type` String,

    `contract_number` String,

    `reference` String,

    `event_code` String,

    `event_description` String,

    `event_office_facilty_id` Int32,

    `event_office_name` String,

    `event_date` DateTime,

    `event_time` DateTime,

    `non_del_reason` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;

-----------------------------5
-- mis_db.mailbooking_dom_customer_xml_mv definition

CREATE TABLE mis_db.mailbooking_dom_customer_xml_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` DateTime,

    `booking_time` DateTime,

    `booking_office_facility_id` Int32,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` String,

    `tariff` Decimal(10,
 2),

    `cod_amount` Float32,

    `booking_type` String,

    `contract_number` String,

    `reference` String,

    `event_code` String,

    `event_description` String,

    `event_office_facilty_id` Int32,

    `event_office_name` String,

    `event_date` DateTime,

    `event_time` DateTime,

    `non_del_reason` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;

---------------------------------------------4
-- mis_db.mailbooking_dom_xml_customer_PC_mv definition

CREATE TABLE mis_db.mailbooking_dom_xml_customer_PC_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

    `booking_office_facility_id` Int32,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` String,

    `tariff` Decimal(10,
 2),

    `cod_amount` Float32,

    `booking_type` String,

    `contract_number` String,

    `reference` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;

------------------------------------3

-- mis_db.mailbooking_dom_customer_PC_mv definition

CREATE TABLE mis_db.mailbooking_dom_customer_PC_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` DateTime,

    `booking_time` DateTime,

    `booking_office_facility_id` Int32,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` Int32,

    `destination_office_name` String,

    `destination_pin` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` String,

    `tariff` Decimal(10,
 2),

    `cod_amount` Float32,

    `booking_type` String,

    `contract_number` String,

    `reference` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;

---------------------------------2
-- mis_db.xml_customer_mv definition

CREATE TABLE mis_db.xml_customer_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

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

    `reference` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;

-----------------1
-- mis_db.xml_PC_test_mv definition

CREATE TABLE mis_db.xml_PC_test_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

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

    `reference` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;