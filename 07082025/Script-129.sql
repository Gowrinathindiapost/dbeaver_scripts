-- mis_db.mv_booking_dom definition

CREATE TABLE mis_db.mv_booking_dom ON CLUSTER cluster_1S_2R
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
ENGINE = ReplicatedMergeTree()
ORDER BY article_number
SETTINGS index_granularity = 8192;


-- mis_db.mv_booking_intl definition

CREATE TABLE mis_db.mv_booking_intl ON CLUSTER cluster_1S_2R
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
ENGINE = ReplicatedMergeTree()
ORDER BY article_number
SETTINGS index_granularity = 8192;


INSERT INTO mis_db.tracking_event_mv (article_number, event_date, event_type, office_id, office_name, source_table, delivery_status, sort_order) VALUES('EF593217261IN', parseDateTimeBestEffort('2025-07-25 12:09:02.000'), 'Item Bagged', 15460001, 'New Delhi NSH', 'ext_bagmgmt_bag_event', '', 3);