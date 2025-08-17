-- `mis_db`.`amazon_target_table` definition

drop table mis_db.amazon_target_table ON CLUSTER cluster_1S_2R

CREATE TABLE mis_db.amazon_target_table ON CLUSTER cluster_1S_2R
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
ENGINE = ReplicatedReplacingMergeTree
PRIMARY KEY (article_number,
 event_date)
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;


CREATE TABLE mis_db.amazon_target_table_ib ON CLUSTER cluster_1S_2R
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
ENGINE = ReplicatedReplacingMergeTree
PRIMARY KEY (article_number,
 event_date)
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;