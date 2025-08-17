
SELECT DISTINCT nctem.*
FROM mis_db.new_customer_xml_facility_customer_mv AS nctem
WHERE nctem.article_number IN (
    SELECT article_number
    FROM mis_db.new_customer_tracking_event_mv
    WHERE event_date > parseDateTime64BestEffort('2025-07-25 00:24:53.952')
      AND event_date <= parseDateTime64BestEffort('2025-07-26 09:24:53.952')
      and event_code='ITEM_BOOK'
    ORDER BY event_date DESC
    LIMIT 1000
)
AND nctem.booking_date > parseDateTime64BestEffort('2025-07-25 00:24:53.952')
AND nctem.booking_date <= parseDateTime64BestEffort('2025-07-26 09:24:53.952')
LIMIT 1000;

-----
CREATE TABLE mis_db.new_customer_xml_facility_customer_mv
ON CLUSTER cluster_1S_2R
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
    `weight_value` Decimal(10,3),
    `tariff` Decimal(10,2),
    `cod_amount` Decimal(10,2),
    `booking_type` String,
    `contract_number` Int32,
    `reference` String,
    `bulk_customer_id` Int64,

    INDEX idx_cxcm_customer_date (bulk_customer_id, booking_date) TYPE minmax GRANULARITY 3,
    INDEX idx_cxcm_article (article_number) TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/mis_db/new_customer_xml_facility_customer_mv', 
    '{replica}', 
    booking_date)
PARTITION BY toYYYYMM(booking_date)
PRIMARY KEY (booking_date, article_number)
ORDER BY (booking_date, article_number)
SETTINGS index_granularity = 8192
;

--------------------
CREATE TABLE mis_db.new_customer_tracking_event_mv
ON CLUSTER cluster_1S_2R
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
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/mis_db/new_customer_tracking_event_mv', '{replica}', event_date)
PARTITION BY toYYYYMM(event_date)
PRIMARY KEY (article_number, event_date)
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192
;


-- mis_db.new_customer_xml_facility_customer_mv definition

CREATE TABLE mis_db.new_customer_xml_facility_customer_mv
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

    INDEX idx_cxcm_customer_date (bulk_customer_id,
 booking_date) TYPE minmax GRANULARITY 3,

    INDEX idx_cxcm_article article_number TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}',
 booking_date)
PARTITION BY toYYYYMM(booking_date)
PRIMARY KEY (booking_date,
 article_number)
ORDER BY (booking_date,
 article_number)
SETTINGS index_granularity = 8192;

DROP TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R;
DROP TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R;

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

    `sort_order` UInt8
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;


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

    `bulk_customer_id` Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;