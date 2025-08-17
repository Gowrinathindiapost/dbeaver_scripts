SELECT
    '<?xml version="1.0" encoding="UTF-8"?>' ||
    '<LatestEventDetails>' ||
    arrayStringConcat(
        arrayFilter(
            x -> x != '',
            arrayMap(
                x -> '<ArticleDetails>' ||
                    '<ArticleNumber>' || ifNull(toString(x.1), '') || '</ArticleNumber>' ||
                    '<ArticleType>' || ifNull(upper(toString(x.2)), '') || '</ArticleType>' ||
                    '<BookingDate>' || ifNull(toString(splitByString(' ', formatDateTime(x.3, '%d%m%Y %H%i%s'))[1]), '') || '</BookingDate>' ||
                    '<BookingTime>' || ifNull(toString(splitByString(' ', formatDateTime(x.3, '%d%m%Y %H%i%s'))[2]), '') || '</BookingTime>' ||
                    '<BookingOfficeName>' || ifNull(upper(toString(x.4)), '') || '</BookingOfficeName>' ||
                    '<BookingOfficePIN>' || ifNull(toString(x.5), '') || '</BookingOfficePIN>' ||
                    '<DestinationPIN>' || ifNull(toString(x.6), '') || '</DestinationPIN>' ||
                    '<DestinationOfficeName>' || ifNull(toString(x.7), '') || '</DestinationOfficeName>' ||
                    '<EventDate>' || ifNull(toString(splitByString(' ', formatDateTime(x.8, '%d%m%Y %H%i%s'))[1]), '') || '</EventDate>' ||
                    '<EventTime>' || ifNull(toString(splitByString(' ', formatDateTime(x.8, '%d%m%Y %H%i%s'))[2]), '') || '</EventTime>' ||
                    '<EventCode>' || ifNull(upper(toString(x.9)), '') || '</EventCode>' ||
                    '<EventDescription>' || ifNull(upper(toString(x.10)), '') || '</EventDescription>' ||
                    '<EventOfficeName>' || ifNull(upper(toString(x.11)), '') || '</EventOfficeName>' ||
                    '<NonDeliveryReason>' || ifNull(upper(toString(x.12)), '') || '</NonDeliveryReason>' ||
                    '</ArticleDetails>',
                groupArray((
                    article_number,
                    article_type,
                    booking_date_time,
                    booking_office_name,
                    booking_office_pin,
                    destination_pin,
                    destination_office_name,
                    art_event_date_time,
                    art_event_code,
                    event_description,
                    event_office_name,
                    non_delivery_reason_description
                ))
            )
        )
    ) ||
    '</LatestEventDetails>' AS xml_output
FROM (
    SELECT *
    FROM mis_db.fct_crm_raw_bulk_last_event
    WHERE customer_id = 1000002954
    AND full_dt BETWEEN parseDateTime64BestEffort('2025-07-30 00:00:00.000000') 
                    AND parseDateTime64BestEffort('2025-07-30 23:59:59.000000')
) AS filtered_data
drop table mis_db.mv_new_latest_event_target ON CLUSTER cluster_1S_2R;

drop table mis_db.amazon_target_table_dt_backup1 ON CLUSTER cluster_1S_2R;
drop table mis_db.amazon_target_table_dt_backup ON CLUSTER cluster_1S_2R;
drop table mis_db.amazon_target_table_dt_backup ON CLUSTER cluster_1S_2R;
drop table mis_db.amazon_target_table_dt1 ON CLUSTER cluster_1S_2R;
drop table mis_db.mv_customer_xml_customer_mv ON CLUSTER cluster_1S_2R;
drop table mis_db.customer_xml_customer_mv ON CLUSTER cluster_1S_2R;
drop table mis_db.latest_customer_tracking_events ON CLUSTER cluster_1S_2R;
drop table mis_db.staging_amazon_with_tracking ON CLUSTER cluster_1S_2R;
drop table mis_db.mv_new_latest_event ON CLUSTER cluster_1S_2R;
drop table mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R;


SYSTEM RESTART REPLICA mis_db.ext_pdmanagement_article_transaction;
---replicas status
SELECT *
FROM system.replicas
WHERE table = 'mv_new_pdmanagement_article_transaction_tracking_event ';
--to find MVs filling target table

SELECT *
FROM system.merges
WHERE table = 'mv_new_pdmanagement_article_transaction_tracking_event '
AND database = 'mis_db';
------------------------------
SELECT
    database,
    name,
    engine,
    create_table_query
FROM system.tables
WHERE engine = 'MaterializedView'
  AND create_table_query ILIKE '%mis_db.latest_customer_tracking_events%';

SHOW CREATE TABLE mis_db.customer_xml_customer_mv;

SELECT
    database,
    name AS mv_name,
    create_table_query
FROM system.tables
WHERE engine = 'MaterializedView';


-- mis_db.customer_xml_customer_mv definition

CREATE TABLE mis_db.customer_xml_customer_mv
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

    `reference` String,

    `bulk_customer_id` Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;
---------------

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
---
CREATE MATERIALIZED VIEW mis_db.mv_customer_xml_customer_mv TO mis_db.customer_xml_customer_mv (`article_number` String, `article_type` String, `booking_date` DateTime64(6), `booking_time` DateTime64(6), `booking_office_facility_id` Int64, `booking_office_name` String, `booking_pin` Int32, `sender_address_city` String, `destination_office_facility_id` Int32, `destination_office_name` String, `destination_pincode` Int32, `destination_city` String, `destination_country` String, `receiver_name` String, `invoice_no` String, `line_item` String, `weight_value` Decimal(10, 3), `tariff` Decimal(10, 2), `cod_amount` Decimal(10, 2), `booking_type` String, `contract_number` Int32, `reference` String, `bulk_customer_id` Int64) AS SELECT kmd.article_number AS article_number, kmd.mail_type_code AS article_type, kmd.md_updated_date AS booking_date, kmd.md_updated_date AS booking_time, kmd.md_office_id_bkg AS booking_office_facility_id, kom.office_name AS booking_office_name, kmd.origin_pincode AS booking_pin, kba1.city AS sender_address_city, kmd.destination_office_id AS destination_office_facility_id, kmd.destination_office_name AS destination_office_name, kmd.destination_pincode AS destination_pincode, kba2.city AS destination_city, 'INDIA' AS destination_country, kba2.addressee_name AS receiver_name, kmd.bkg_ref_id AS invoice_no, '' AS line_item, kmd.charged_weight AS weight_value, kcd.total_amount AS tariff, kcd.vp_cod_value AS cod_amount, kmd.booking_type_code AS booking_type, kmd.contract_id AS contract_number, '' AS reference, kmd.bulk_customer_id AS bulk_customer_id FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0)
------------------
-- mis_db.staging_amazon_with_tracking definition

CREATE TABLE mis_db.staging_amazon_with_tracking
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

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `delivery_status` String
)
ENGINE = MergeTree
ORDER BY article_number
SETTINGS index_granularity = 8192;
-----------------------
-- mis_db.new_customer_tracking_event_mv definition

CREATE TABLE mis_db.new_customer_tracking_event_mv
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
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}',
 event_date)
PARTITION BY toYYYYMM(event_date)
PRIMARY KEY (article_number,
 event_date)
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;
---------------------------
-- mis_db.mv_new_latest_event source

CREATE MATERIALIZED VIEW mis_db.mv_new_latest_event TO mis_db.mv_new_latest_event_target
(

    `article_number` String,

    `event_date_state` AggregateFunction(max,
 DateTime),

    `event_code_state` AggregateFunction(argMax,
 String,
 DateTime),

    `event_type_state` AggregateFunction(argMax,
 String,
 DateTime),

    `office_id_state` AggregateFunction(argMax,
 String,
 DateTime),

    `office_name_state` AggregateFunction(argMax,
 String,
 DateTime),

    `delivery_status_state` AggregateFunction(argMax,
 String,
 DateTime)
)
AS SELECT
    article_number,

    maxState(toDateTime(event_date)) AS event_date_state,

    argMaxState(event_code,
 toDateTime(event_date)) AS event_code_state,

    argMaxState(event_type,
 toDateTime(event_date)) AS event_type_state,

    argMaxState(office_id,
 toDateTime(event_date)) AS office_id_state,

    argMaxState(office_name,
 toDateTime(event_date)) AS office_name_state,

    argMaxState(delivery_status,
 toDateTime(event_date)) AS delivery_status_state
FROM mis_db.new_customer_tracking_event_mv
GROUP BY article_number;