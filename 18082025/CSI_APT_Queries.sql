
-----------------------------------1
-- mis_db.csi_article_item definition

CREATE TABLE mis_db.csi_article_item ON CLUSTER cluster_1S_2R
(

    `db_id` UUID DEFAULT generateUUIDv4(),

    `bag_id` Nullable(String),

    `bag_item` Nullable(String),

    `article_type` Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (assumeNotNull(bag_id),
 assumeNotNull(bag_item))
SETTINGS index_granularity = 8192;


----------------2
-- mis_db.csi_article_event definition

CREATE TABLE mis_db.csi_article_event ON CLUSTER cluster_1S_2R
(

    `article_number` Nullable(String),

    `status` Nullable(String),

    `event_date` Nullable(DateTime),

    `event_time` Nullable(String),

    `event_ofc_facility_id` Nullable(String),

    `to_facility_id` Nullable(String),

    `weight_gms` Nullable(UInt32),

    `article_type` Nullable(String),

    `reason_code` Nullable(String),

    `action_code` Nullable(String),

    `user_id` Nullable(String),

    `version` DateTime DEFAULT now(),

    `db_id` UInt64 MATERIALIZED reinterpretAsUInt64(cityHash64(generateUUIDv4()))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(assumeNotNull(event_date))
ORDER BY (assumeNotNull(article_number),
 assumeNotNull(status),
 assumeNotNull(event_date),
 assumeNotNull(event_time),
 assumeNotNull(event_ofc_facility_id))
SETTINGS index_granularity = 8192;
------------------------3


-- mis_db.csi_bag_header definition

CREATE TABLE mis_db.csi_bag_header ON CLUSTER cluster_1S_2R
(

    `db_id` UInt64,

    `bag_id` String,

    `status` String,

    `event_date` DateTime,

    `event_time` String,

    `at_facility_id` String,

    `at_office_name` String,

    `from_to_facility_id` String,

    `from_to_office_name` String,

    `bag_type` String,

    `bag_weight` Float32,

    `created_by` String
)
ENGINE = ReplacingMergeTree
ORDER BY (bag_id,
 status,
 event_date)
SETTINGS index_granularity = 8192;
--------------