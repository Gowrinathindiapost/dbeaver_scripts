SELECT
    cxcm.article_number,
    cxcm.article_type,
    formatDateTime(cxcm.booking_date, '%d%m%Y') AS booking_date,
    formatDateTime(cxcm.booking_time, '%H%i%s') AS booking_time,
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

    -- Aggregated columns from materialized view
    formatDateTime(maxMerge(te.event_date_state), '%d%m%Y') AS event_date,
    formatDateTime(maxMerge(te.event_date_state), '%H%i%s') AS event_time,
    argMaxMerge(te.event_code_state) AS event_code,
    argMaxMerge(te.event_type_state) AS event_type,
    argMaxMerge(te.office_id_state) AS office_id,
    argMaxMerge(te.office_name_state) AS office_name,
    argMaxMerge(te.delivery_status_state) AS delivery_status

FROM mis_db.new_customer_xml_facility_customer_mv cxcm
LEFT JOIN mis_db.mv_new_latest_event_target te
    ON cxcm.article_number = te.article_number
--    AND te.event_date >= parseDateTime64BestEffort('2025-07-01')
--  AND te.event_date < parseDateTime64BestEffort('2025-07-25')
--  WHERE cxcm.bulk_customer_id = 1000002954
--  AND cxcm.booking_date < now()

GROUP BY
    cxcm.article_number,
    cxcm.article_type,
    cxcm.booking_date,
    cxcm.booking_time,
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
    cxcm.bulk_customer_id;


SELECT
    cxcm.article_number,
    cxcm.article_type,
    formatDateTime(cxcm.booking_date, '%d%m%Y') AS booking_date,
    formatDateTime(cxcm.booking_time, '%H%i%s') AS booking_time,
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

    -- Finalized event fields
    formatDateTime(maxMerge(te.event_date_state), '%d%m%Y') AS event_date,
    formatDateTime(maxMerge(te.event_date_state), '%H%i%s') AS event_time,
    argMaxMerge(te.event_code_state) AS event_code,
    argMaxMerge(te.event_type_state) AS event_type,
    argMaxMerge(te.office_id_state) AS office_id,
    argMaxMerge(te.office_name_state) AS office_name,
    argMaxMerge(te.delivery_status_state) AS delivery_status

FROM mis_db.new_customer_xml_facility_customer_mv cxcm
LEFT JOIN mis_db.mv_new_latest_event_target te
    ON cxcm.article_number = te.article_number
WHERE
    cxcm.bulk_customer_id = 1000002954
    AND cxcm.booking_date < now()
GROUP BY
    cxcm.article_number,
cxcm.article_type,
    cxcm.booking_date,
    cxcm.booking_time,
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
    cxcm.bulk_customer_id;

HAVING
    maxMerge(te.event_date_state) >= parseDateTime64BestEffort('2025-07-01')
    AND maxMerge(te.event_date_state) < parseDateTime64BestEffort('2025-07-25')
--CREATE MATERIALIZED VIEW mis_db.mv_new_latest_event
--ENGINE = AggregatingMergeTree
--PARTITION BY toYYYYMM(event_date)
--ORDER BY article_number AS
--SELECT
--    article_number,
--    max(event_date) AS event_date,
--    argMax(event_code, event_date) AS event_code,
--    argMax(event_type, event_date) AS event_type,
--    argMax(office_id, event_date) AS office_id,
--    argMax(office_name, event_date) AS office_name,
--    argMax(delivery_status, event_date) AS delivery_status
--FROM mis_db.new_customer_tracking_event_mv
--GROUP BY article_number;
----
    drop table mis_db.mv_new_latest_event
CREATE MATERIALIZED VIEW mis_db.mv_new_latest_event
TO mis_db.mv_new_latest_event_target
AS
SELECT
    article_number,
    maxState(toDateTime(event_date)) AS event_date_state,
    argMaxState(event_code, toDateTime(event_date)) AS event_code_state,
    argMaxState(event_type, toDateTime(event_date)) AS event_type_state,
    argMaxState(office_id, toDateTime(event_date)) AS office_id_state,
    argMaxState(office_name, toDateTime(event_date)) AS office_name_state,
    argMaxState(delivery_status, toDateTime(event_date)) AS delivery_status_state
FROM mis_db.new_customer_tracking_event_mv
GROUP BY article_number;



SELECT
    article_number,
    maxMerge(event_date_state) AS event_date,
    argMaxMerge(event_code_state) AS event_code,
    argMaxMerge(event_type_state) AS event_type,
    argMaxMerge(office_id_state) AS office_id,
    argMaxMerge(office_name_state) AS office_name,
    argMaxMerge(delivery_status_state) AS delivery_status
FROM mis_db.mv_new_latest_event_target
GROUP BY article_number

SELECT uniq(article_number) FROM mis_db.new_customer_tracking_event_mv;

INSERT INTO mis_db.mv_new_latest_event_target
SELECT
    article_number,
    maxState(toDateTime(event_date)) AS event_date_state,
    argMaxState(event_code, toDateTime(event_date)) AS event_code_state,
    argMaxState(event_type, toDateTime(event_date)) AS event_type_state,
    argMaxState(office_id, toDateTime(event_date)) AS office_id_state,
    argMaxState(office_name, toDateTime(event_date)) AS office_name_state,
    argMaxState(delivery_status, toDateTime(event_date)) AS delivery_status_state
FROM mis_db.new_customer_tracking_event_mv
GROUP BY article_number;


--INSERT INTO mis_db.mv_new_latest_event_target
--SELECT
--    article_number,
--    maxState(event_date) AS event_date_state,
--    argMaxState(event_code, event_date) AS event_code_state,
--    argMaxState(event_type, event_date) AS event_type_state,
--    argMaxState(office_id, event_date) AS office_id_state,
--    argMaxState(office_name, event_date) AS office_name_state,
--    argMaxState(delivery_status, event_date) AS delivery_status_state
--FROM mis_db.new_customer_tracking_event_mv
--GROUP BY article_number;
----
drop table mis_db.mv_new_latest_event_target

-- Drop the old table if it exists (be careful!):
-- DROP TABLE mis_db.mv_new_latest_event_target
DROP TABLE IF EXISTS mis_db.mv_new_latest_event_target ON CLUSTER cluster_1S_2R;
CREATE TABLE mis_db.mv_new_latest_event_target ON CLUSTER cluster_1S_2R
(
    `article_number` String,
    `event_date_state` AggregateFunction(max, DateTime),
    `event_code_state` AggregateFunction(argMax, String, DateTime),
    `event_type_state` AggregateFunction(argMax, String, DateTime),
    `office_id_state` AggregateFunction(argMax, String, DateTime),
    `office_name_state` AggregateFunction(argMax, String, DateTime),
    `delivery_status_state` AggregateFunction(argMax, String, DateTime)
)
ENGINE = AggregatingMergeTree()
PARTITION BY tuple()
ORDER BY article_number;

--CREATE TABLE mis_db.mv_new_latest_event_target ON CLUSTER cluster_1S_2R
--(
--    `article_number` String,
--    `event_date_state` AggregateFunction(max, DateTime64(6)),
--    `event_code_state` AggregateFunction(argMax, String, DateTime64(6)),
--    `event_type_state` AggregateFunction(argMax, String, DateTime64(6)),
--    `office_id_state` AggregateFunction(argMax, String, DateTime64(6)),
--    `office_name_state` AggregateFunction(argMax, String, DateTime64(6)),
--    `delivery_status_state` AggregateFunction(argMax, String, DateTime64(6))
--)
--ENGINE = AggregatingMergeTree()
--PARTITION BY tuple()
--ORDER BY article_number;

--CREATE TABLE mis_db.mv_new_latest_event_target
--(
--    `article_number` String,
--    `event_date_state` AggregateFunction(max, DateTime),
--    `event_code_state` AggregateFunction(argMax, String, DateTime),
--    `event_type_state` AggregateFunction(argMax, String, DateTime),
--    `office_id_state` AggregateFunction(argMax, String, DateTime),
--    `office_name_state` AggregateFunction(argMax, String, DateTime),
--    `delivery_status_state` AggregateFunction(argMax, String, DateTime)
--)
--ENGINE = AggregatingMergeTree()
--PARTITION BY tuple()
--ORDER BY article_number;



----to see running queries
SELECT 
    query_id,
    user,
    address,
    elapsed,
    read_rows,
    read_bytes,
    written_rows,
    written_bytes,
    memory_usage,
    query
FROM system.processes
ORDER BY elapsed DESC;



SHOW CREATE TABLE mis_db.new_customer_xml_facility_customer_new_mv
SHOW CREATE TABLE mis_db.ext_mailbkg_mailbooking_dom;

-----------------------26
-- mis_db.mv_new_pdmanagement_article_transaction_tracking_event source

drop table mis_db.mv_new_pdmanagement_article_transaction_tracking_event

CREATE MATERIALIZED VIEW mis_db.mv_new_pdmanagement_article_transaction_tracking_event TO mis_db.new_customer_tracking_event_mv
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
insert into mis_db.new_customer_tracking_event_mv
SELECT distinct
    article_number,

    --now() AS event_date,
     now64(6) AS event_date,

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
drop table mis_db.mv_new_customer_xml_facility_customer_mv

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
AS 
insert into mis_db.new_customer_xml_facility_customer_mv
SELECT distinct
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    kmd.md_updated_date  AS booking_date,

  kmd.md_updated_date  AS booking_time,

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
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.destination_office_id = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0) 


select * from mis_db.ext_mailbkg_mailbooking_dom AS kmd
-- `mis_db`.`mv_new_booking_dom_to_tracking_event` source

CREATE MATERIALIZED VIEW mis_db.mv_new_booking_dom_to_tracking_event ON CLUSTER cluster_1S_2R  
TO mis_db.new_customer_tracking_event_mv
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
insert into mis_db.new_customer_tracking_event_mv
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
AS 
insert into mis_db.new_customer_tracking_event_mv
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
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content AS bcc ON (be.bag_number = bcc.bag_number) AND (be.event_type IN ('CL',
 'DI'))
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content AS boc ON (be.bag_number = boc.bag_number) AND (be.event_type IN ('OP',
 'OR'))
LEFT JOIN mis_db.ext_mdm_office_master AS kom ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 ON be.to_office_id = kom2.office_id


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
AS 
insert into mis_db.new_customer_tracking_event_mv
SELECT --distinct
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

--------------------------------22
-- mis_db.new_customer_xml_facility_customer_mv definition

CREATE TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R
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

    `booking_date_parsed` DateTime MATERIALIZED parseDateTimeBestEffort(booking_date),

    INDEX idx_cxcm_customer_date (bulk_customer_id,
 parseDateTimeBestEffort(booking_date)) TYPE minmax GRANULARITY 3,

    INDEX idx_cxcm_article article_number TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplicatedReplacingMergeTree(booking_date)
PARTITION BY toYYYYMM(booking_date)
PRIMARY KEY (booking_date,article_number)
ORDER BY (booking_date,article_number)
SETTINGS index_granularity = 8192;

----new
CREATE TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R
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
    `weight_value` Decimal(10, 3),
    `tariff` Decimal(10, 2),
    `cod_amount` Decimal(10, 2),
    `booking_type` String,
    `contract_number` Int32,
    `reference` String,
    `bulk_customer_id` Int64,

    INDEX idx_cxcm_customer_date (bulk_customer_id, booking_date) TYPE minmax GRANULARITY 3,
    INDEX idx_cxcm_article (article_number) TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplicatedReplacingMergeTree(booking_date)
PARTITION BY toYYYYMM(booking_date)
PRIMARY KEY (booking_date, article_number)
ORDER BY (booking_date, article_number)
SETTINGS index_granularity = 8192;

---------------------------21
SET max_table_size_to_drop = 80000000000; -- 80 GB

DROP TABLE mis_db.new_customer_tracking_event_mv;


-- mis_db.new_customer_tracking_event_mv definition

CREATE TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R
(

    `article_number` String,

    `event_date` DateTime64(6),

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
ENGINE = ReplicatedReplacingMergeTree(event_date)
 PRIMARY KEY (article_number,
 event_date)
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;

--new
CREATE TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R
(
    `article_number` String,
    `event_date` DateTime64(6),
    `event_type` String,
    `event_code` String,
    `office_id` String,
    `office_name` String,
    `source_table` String,
    `delivery_status` String,
    `sort_order` UInt8,

    INDEX idx_te_date_code (event_date, event_code) TYPE minmax GRANULARITY 3,
    INDEX idx_te_article_date_code (article_number, event_date, event_code) TYPE minmax GRANULARITY 3,
    INDEX idx_te_date (event_date) TYPE minmax GRANULARITY 3,
    INDEX idx_te_article_date (article_number, event_date) TYPE minmax GRANULARITY 3
)
ENGINE = ReplicatedReplacingMergeTree(event_date)
PARTITION BY toYYYYMM(event_date)
PRIMARY KEY (article_number, event_date)
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;


