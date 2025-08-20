
-------------------2

--drop TABLE mis_db.new_customer_tracking_event_new_mv ON CLUSTER cluster_1S_2R;
CREATE TABLE mis_db.new_customer_tracking_event_new_mv ON CLUSTER cluster_1S_2R
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
    INDEX idx_te_date event_date TYPE minmax GRANULARITY 3,
    INDEX idx_te_article_date (article_number, event_date) TYPE minmax GRANULARITY 3,
    INDEX idx_te_article article_number TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', event_date)
PARTITION BY toYYYYMM(event_date)
PRIMARY KEY (article_number, event_date)
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;


--------------------------1
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
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}',
 event_date)
PARTITION BY toYYYYMM(event_date)
PRIMARY KEY (article_number,
 event_date)
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;


