



--------------------------------3
-- mis_db.mv_booking_intl definition

CREATE TABLE mis_db.mv_booking_intl
(

    `article_number` String,

    `bookedat` String,

    `bookedon` DateTime,

    `source_country` String,

    `destination_country` String,

    `delivery_location` String,

    `article_type` String,

    `tariff` Decimal(10,
 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;
-------------------------2

-- mis_db.mv_booking_dom definition

CREATE TABLE mis_db.mv_booking_dom
(

    `article_number` String,

    `bookedat` String,

    `bookedon` DateTime,

    `destination_pincode` Int32,

    `delivery_location` String,

    `article_type` String,

    `tariff` Decimal(10,
 2)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY article_number
SETTINGS index_granularity = 8192;
--------1
-- mis_db.tracking_event_mv definition

CREATE TABLE mis_db.tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime,

    `event_type` String,

    `office_id` Int32,

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