
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

-------------17
-- mis_db.new_customer_xml_facility_customer_new_mv definition

rename TABLE mis_db.new_customer_xml_facility_customer_new_mv to mis_db.new_customer_xml_facility_customer_new_mv_bkup_1708 ON CLUSTER cluster_1S_2R;

CREATE TABLE mis_db.new_customer_xml_facility_customer_new_mv ON CLUSTER cluster_1S_2R
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

    INDEX idx_cxcm_customer_booking (bulk_customer_id,
 booking_date) TYPE minmax GRANULARITY 3,

    INDEX idx_cxcm_article article_number TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}',
 booking_date)
PRIMARY KEY (booking_date,
 article_number)
ORDER BY (booking_date,
 article_number)
SETTINGS index_granularity = 8192;