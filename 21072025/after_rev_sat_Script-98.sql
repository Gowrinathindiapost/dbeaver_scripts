-- `mis_db`.`new_customer_tracking_event_mv` definition

CREATE TABLE mis_db.new_customer_tracking_event_new_mv ON CLUSTER cluster_1S_2R
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
ENGINE = ReplicatedReplacingMergeTree(event_date)
PRIMARY KEY (article_number,
 event_date)
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;

-- `mis_db`.`new_customer_xml_facility_customer_mv` definition

CREATE TABLE mis_db.new_customer_xml_facility_customer_new_mv ON CLUSTER cluster_1S_2R
(

    `article_number` String,

    `article_type` String,

    `booking_date`  DateTime64(6),

    `booking_time`  DateTime64(6),

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
ENGINE = ReplicatedReplacingMergeTree(booking_date)
PRIMARY KEY (booking_date,article_number)
ORDER BY (booking_date,article_number)
SETTINGS index_granularity = 8192;

-- `mis_db`.`mv_new_article_events_tracking_event` source

CREATE MATERIALIZED VIEW mis_db.mv_new_article_events_tracking_event_new ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_tracking_event_new_mv
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
AS 
insert into mis_db.new_customer_tracking_event_new_mv
SELECT
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
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kae.current_office_id = kom.office_id;

-- `mis_db`.`mv_new_bag_events_tracking_event` source

CREATE MATERIALIZED VIEW mis_db.mv_new_bag_events_tracking_event_new ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_tracking_event_new_mv
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
AS 
insert into mis_db.new_customer_tracking_event_new_mv
SELECT distinct
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
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content AS bcc ON (be.bag_number = bcc.bag_number) AND (be.event_type IN ('CL', 'DI'))
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content AS boc ON (be.bag_number = boc.bag_number) AND (be.event_type IN ('OP', 'OR'))
LEFT JOIN mis_db.ext_mdm_office_master AS kom ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 ON be.to_office_id = kom2.office_id;


-- `mis_db`.`mv_new_booking_dom_to_tracking_event` source

CREATE MATERIALIZED VIEW mis_db.mv_new_booking_dom_to_tracking_event_new ON CLUSTER cluster_1S_2R  
TO mis_db.new_customer_tracking_event_new_mv
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
AS --pending
insert into mis_db.new_customer_tracking_event_new_mv
SELECT distinct
    kmd.article_number AS article_number,

    kmd.md_updated_date AS event_date,

    'Item Book' AS event_type,

    'ITEM_BOOK' AS event_code,

    kom.csi_facility_id AS office_id,

    kom.office_name AS office_name,

    'ext_mailbkg_mailbooking_dom' AS source_table,

    '' AS delivery_status,

    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_dom AS  kmd 
INNER JOIN mis_db.ext_mdm_office_master AS kom  ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);


-- `mis_db`.`mv_new_customer_xml_facility_customer_mv` source

CREATE MATERIALIZED VIEW mis_db.mv_new_customer_xml_facility_customer_mv_new ON CLUSTER cluster_1S_2R 
TO mis_db.new_customer_xml_facility_customer_new_mv
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
AS 
insert into mis_db.new_customer_xml_facility_customer_new_mv
SELECT distinct
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
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id and kmd.sender_pincode = kba1.pincode
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id and kmd.receiver_pincode = kba2.pincode
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd  ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom  ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon  ON kmd.destination_office_id = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);


-- `mis_db`.`mv_new_pdmanagement_article_transaction_tracking_event` source

CREATE MATERIALIZED VIEW mis_db.mv_new_pdmanagement_article_transaction_tracking_event_new ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_tracking_event_new_mv
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
AS 
insert into mis_db.new_customer_tracking_event_new_mv
SELECT
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