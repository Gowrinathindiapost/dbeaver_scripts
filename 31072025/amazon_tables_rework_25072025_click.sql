-- mis_db.amazon_target_table_dt definition

CREATE TABLE mis_db.amazon_target_table_dt1 ON CLUSTER cluster_1S_2R
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

    `event_time` DateTime64(6),

    `event_code` String,

    `event_office_facilty_id` String,

    `office_name` String,

    `event_description` String,

    `non_delivery_reason` String
)
ENGINE = ReplicatedReplacingMergeTree(event_date)
PRIMARY KEY (article_number,
 event_date)
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;
---------------------
CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv TO mis_db.amazon_target_table_dt
AS
WITH latest_events AS (
    SELECT
        article_number,
        -- Use argMax directly to get the tuple for the latest event_date
        argMax(
            (event_date, event_code, office_id, office_name, event_type, delivery_status),
            event_date
        ) as latest_event_tuple
    FROM mis_db.new_customer_tracking_event_new_mv
    GROUP BY article_number
)
SELECT
    cxcm.article_number AS article_number,
    cxcm.article_type AS article_type,
    cxcm.booking_date AS booking_date,
    cxcm.booking_time AS booking_time,
    cxcm.booking_office_facility_id AS booking_office_facility_id,
    cxcm.booking_office_name AS booking_office_name,
    cxcm.booking_pin AS booking_pin,
    cxcm.sender_address_city AS sender_address_city,
    cxcm.destination_office_facility_id AS destination_office_facility_id,
    cxcm.destination_office_name AS destination_office_name,
    cxcm.destination_pincode AS destination_pincode,
    cxcm.destination_city AS destination_city,
    cxcm.destination_country AS destination_country,
    cxcm.receiver_name AS receiver_name,
    cxcm.invoice_no AS invoice_no,
    cxcm.line_item AS line_item,
    cxcm.weight_value AS weight_value,
    cxcm.tariff AS tariff,
    cxcm.cod_amount AS cod_amount,
    cxcm.booking_type AS booking_type,
    cxcm.contract_number AS contract_number,
    cxcm.reference AS reference,
    cxcm.bulk_customer_id AS bulk_customer_id,
    -- Access tuple elements directly using dot notation
    latest_events.latest_event_tuple.1 AS event_date,
    latest_events.latest_event_tuple.1 AS event_time, -- Assuming event_time is the same as event_date for simplicity, adjust if needed
    latest_events.latest_event_tuple.2 AS event_code,
    latest_events.latest_event_tuple.3 AS event_office_facilty_id,
    latest_events.latest_event_tuple.4 AS office_name,
    latest_events.latest_event_tuple.5 AS event_description,
    latest_events.latest_event_tuple.6 AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
LEFT JOIN latest_events ON cxcm.article_number = latest_events.article_number
WHERE (cxcm.booking_date < now());
WHERE (cxcm.bulk_customer_id != 0) AND (cxcm.booking_date < now());


----
CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv TO mis_db.amazon_target_table_dt
AS 

WITH latest_events AS (
    SELECT
        article_number,
        -- Use argMax directly to get the tuple for the latest event_date
        argMax(
            (event_date, event_code, office_id, office_name, event_type, delivery_status),
            event_date
        ) as latest_event_tuple
    FROM mis_db.new_customer_tracking_event_new_mv
    GROUP BY article_number
)
SELECT
    cxcm.article_number AS article_number,
    cxcm.article_type AS article_type,
    cxcm.booking_date AS booking_date,
    cxcm.booking_time AS booking_time,
    cxcm.booking_office_facility_id AS booking_office_facility_id,
    cxcm.booking_office_name AS booking_office_name,
    cxcm.booking_pin AS booking_pin,
    cxcm.sender_address_city AS sender_address_city,
    cxcm.destination_office_facility_id AS destination_office_facility_id,
    cxcm.destination_office_name AS destination_office_name,
    cxcm.destination_pincode AS destination_pincode,
    cxcm.destination_city AS destination_city,
    cxcm.destination_country AS destination_country,
    cxcm.receiver_name AS receiver_name,
    cxcm.invoice_no AS invoice_no,
    cxcm.line_item AS line_item,
    cxcm.weight_value AS weight_value,
    cxcm.tariff AS tariff,
    cxcm.cod_amount AS cod_amount,
    cxcm.booking_type AS booking_type,
    cxcm.contract_number AS contract_number,
    cxcm.reference AS reference,
    cxcm.bulk_customer_id AS bulk_customer_id,
    -- Access tuple elements directly using dot notation
    latest_events.latest_event_tuple.1 AS event_date,
    latest_events.latest_event_tuple.1 AS event_time, -- Assuming event_time is the same as event_date for simplicity, adjust if needed
    latest_events.latest_event_tuple.2 AS event_code,
    latest_events.latest_event_tuple.3 AS event_office_facilty_id,
    latest_events.latest_event_tuple.4 AS office_name,
    latest_events.latest_event_tuple.5 AS event_description,
    latest_events.latest_event_tuple.6 AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
LEFT JOIN latest_events ON cxcm.article_number = latest_events.article_number
WHERE (cxcm.bulk_customer_id != 0) AND (cxcm.booking_date < now());



drop table mis_db.staging_amazon_with_tracking_mv ON CLUSTER cluster_1S_2R


--ds1
CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv TO mis_db.amazon_target_table_dt
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
    `event_date` DateTime64(6),
    `event_time` DateTime64(6),
    `event_code` String,
    `event_office_facilty_id` String,
    `office_name` String,
    `event_description` String,
    `non_delivery_reason` String
)
AS 
insert into mis_db.amazon_target_table_dt
SELECT
    cxcm.article_number AS article_number,
    cxcm.article_type AS article_type,
    cxcm.booking_date AS booking_date,
    cxcm.booking_time AS booking_time,
    cxcm.booking_office_facility_id AS booking_office_facility_id,
    cxcm.booking_office_name AS booking_office_name,
    cxcm.booking_pin AS booking_pin,
    cxcm.sender_address_city AS sender_address_city,
    cxcm.destination_office_facility_id AS destination_office_facility_id,
    cxcm.destination_office_name AS destination_office_name,
    cxcm.destination_pincode AS destination_pincode,
    cxcm.destination_city AS destination_city,
    cxcm.destination_country AS destination_country,
    cxcm.receiver_name AS receiver_name,
    cxcm.invoice_no AS invoice_no,
    cxcm.line_item AS line_item,
    cxcm.weight_value AS weight_value,
    cxcm.tariff AS tariff,
    cxcm.cod_amount AS cod_amount,
    cxcm.booking_type AS booking_type,
    cxcm.contract_number AS contract_number,
    cxcm.reference AS reference,
    cxcm.bulk_customer_id AS bulk_customer_id,
    latest_event.event_date AS event_date,
    latest_event.event_date AS event_time,
    latest_event.event_code AS event_code,
    latest_event.office_id AS event_office_facilty_id,
    latest_event.office_name AS office_name,
    latest_event.event_type AS event_description,
    latest_event.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
INNER JOIN (
    SELECT 
        article_number,
        argMax(
            (event_date, event_code, office_id, office_name, event_type, delivery_status),
            event_date
        ) AS latest_event_tuple
    FROM mis_db.new_customer_tracking_event_new_mv
    GROUP BY article_number
) AS max_dates ON cxcm.article_number = max_dates.article_number
LEFT JOIN (
    SELECT 
        article_number,
        latest_event_tuple.1 AS event_date,
        latest_event_tuple.2 AS event_code,
        latest_event_tuple.3 AS office_id,
        latest_event_tuple.4 AS office_name,
        latest_event_tuple.5 AS event_type,
        latest_event_tuple.6 AS delivery_status
    FROM (
        SELECT 
            article_number,
            argMax(
                (event_date, event_code, office_id, office_name, event_type, delivery_status),
                event_date
            ) AS latest_event_tuple
        FROM mis_db.new_customer_tracking_event_new_mv
        GROUP BY article_number
    )
) AS latest_event ON cxcm.article_number = latest_event.article_number
WHERE (cxcm.bulk_customer_id != 0) AND (cxcm.booking_date < now());
--below is test1 from DS
CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv TO mis_db.amazon_target_table_dt
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
    `event_date` DateTime64(6),
    `event_time` DateTime64(6),
    `event_code` String,
    `event_office_facilty_id` String,
    `office_name` String,
    `event_description` String,
    `non_delivery_reason` String
)
AS 
SELECT
    cxcm.article_number AS article_number,

    cxcm.article_type AS article_type,

    cxcm.booking_date AS booking_date,

    cxcm.booking_time AS booking_time,

    cxcm.booking_office_facility_id AS booking_office_facility_id,

    cxcm.booking_office_name AS booking_office_name,

    cxcm.booking_pin AS booking_pin,

    cxcm.sender_address_city AS sender_address_city,

    cxcm.destination_office_facility_id AS destination_office_facility_id,

    cxcm.destination_office_name AS destination_office_name,

    cxcm.destination_pincode AS destination_pincode,

    cxcm.destination_city AS destination_city,

    cxcm.destination_country AS destination_country,

    cxcm.receiver_name AS receiver_name,

    cxcm.invoice_no AS invoice_no,

    cxcm.line_item AS line_item,

    cxcm.weight_value AS weight_value,

    cxcm.tariff AS tariff,

    cxcm.cod_amount AS cod_amount,

    cxcm.booking_type AS booking_type,

    cxcm.contract_number AS contract_number,

    cxcm.reference AS reference,

    cxcm.bulk_customer_id AS bulk_customer_id,

    le.event_date AS event_date,

   le.event_date AS event_time,

    le.event_code AS event_code,

    le.office_id AS event_office_facilty_id,

    le.office_name AS office_name,

    le.event_type AS event_description,

    le.delivery_status AS non_delivery_reason
    FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
LEFT JOIN (
    SELECT 
        article_number,
        argMax(event_date, event_date) AS max_event_date
    FROM mis_db.new_customer_tracking_event_new_mv
    GROUP BY article_number
) AS max_dates ON cxcm.article_number = max_dates.article_number
LEFT JOIN mis_db.new_customer_tracking_event_new_mv AS le 
    ON le.article_number = max_dates.article_number 
    AND le.event_date = max_dates.max_event_date
WHERE (cxcm.bulk_customer_id != 0) AND (cxcm.booking_date < now());
--DS
CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv TO mis_db.amazon_target_table_dt
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
    `event_date` DateTime64(6),
    `event_time` DateTime64(6),
    `event_code` String,
    `event_office_facilty_id` String,
    `office_name` String,
    `event_description` String,
    `non_delivery_reason` String
)
AS 
SELECT DISTINCT
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
    cxcm.bulk_customer_id,
    latest_event.event_date,
    latest_event.event_date AS event_time,
    latest_event.event_code,
    latest_event.office_id AS event_office_facilty_id,
    latest_event.office_name,
    latest_event.event_type AS event_description,
    latest_event.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
LEFT JOIN (
    SELECT 
        article_number,
        argMax(event_date, event_date) AS max_event_date
    FROM mis_db.new_customer_tracking_event_new_mv
    GROUP BY article_number
) AS max_dates ON cxcm.article_number = max_dates.article_number
LEFT JOIN mis_db.new_customer_tracking_event_new_mv AS latest_event 
    ON latest_event.article_number = max_dates.article_number 
    AND latest_event.event_date = max_dates.max_event_date
WHERE (cxcm.bulk_customer_id != 0) AND (cxcm.booking_date < now());
---------------------
CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv
TO mis_db.amazon_target_table_dt
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

    `event_time` DateTime64(6),

    `event_code` String,

    `event_office_facilty_id` String,

    `office_name` String,

    `event_description` String,

    `non_delivery_reason` String
)
AS
--insert into mis_db.amazon_target_table_dt
SELECT DISTINCT
    cxcm.article_number AS article_number,

    cxcm.article_type AS article_type,

    cxcm.booking_date AS booking_date,

    cxcm.booking_time AS booking_time,

    cxcm.booking_office_facility_id AS booking_office_facility_id,

    cxcm.booking_office_name AS booking_office_name,

    cxcm.booking_pin AS booking_pin,

    cxcm.sender_address_city AS sender_address_city,

    cxcm.destination_office_facility_id AS destination_office_facility_id,

    cxcm.destination_office_name AS destination_office_name,

    cxcm.destination_pincode AS destination_pincode,

    cxcm.destination_city AS destination_city,

    cxcm.destination_country AS destination_country,

    cxcm.receiver_name AS receiver_name,

    cxcm.invoice_no AS invoice_no,

    cxcm.line_item AS line_item,

    cxcm.weight_value AS weight_value,

    cxcm.tariff AS tariff,

    cxcm.cod_amount AS cod_amount,

    cxcm.booking_type AS booking_type,

    cxcm.contract_number AS contract_number,

    cxcm.reference AS reference,

    cxcm.bulk_customer_id AS bulk_customer_id,

    te.event_date AS event_date,

    toDateTime64(te.event_date,
 6) + toIntervalSecond(0) AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
INNER JOIN mis_db.new_customer_tracking_event_new_mv AS te
    ON cxcm.article_number = te.article_number
WHERE (cxcm.bulk_customer_id != 0) AND (te.article_number IS NOT NULL); --and article_number='JD420257766IN';
---below is from ct
CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv
TO mis_db.amazon_target_table_dt
AS
SELECT
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
    cxcm.bulk_customer_id,
    te.event_date,
    te.event_date AS event_time,
    te.event_code,
    te.office_id AS event_office_facilty_id,
    te.office_name,
    te.event_type AS event_description,
    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
INNER JOIN mis_db.new_customer_tracking_event_new_mv AS te
    ON cxcm.article_number = te.article_number
WHERE (cxcm.bulk_customer_id != 0) AND (te.article_number IS NOT NULL);

select * from mis_db.new_customer_tracking_event_new_mv where article_number='JD420257766IN'
select * from mis_db.ext_mailbkg_mailbooking_dom where article_number='JD420257766IN'
---new script below ct
CREATE TABLE mis_db.amazon_target_table_dt ON CLUSTER cluster_1S_2R
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
    `event_date` DateTime64(6),
    `event_time` DateTime64(6),
    `event_code` String,
    `event_office_facilty_id` String,
    `office_name` String,
    `event_description` String,
    `non_delivery_reason` String
)
ENGINE = ReplicatedReplacingMergeTree(event_date)
PRIMARY KEY (article_number, event_date)
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;
--ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
--ORDER BY (article_number, event_date)
--PRIMARY KEY (article_number, event_date)
--VERSION event_time
--SETTINGS index_granularity = 8192;
----old script below 

RENAME TABLE mis_db.amazon_target_table_dt TO mis_db.amazon_target_table_dt_backup1;
RENAME TABLE mis_db.amazon_target_table_dt_mv TO mis_db.amazon_target_table_dt_mv_backup1;
drop table mis_db.amazon_target_table_dt_mv_backup ON CLUSTER cluster_1S_2R
drop table mis_db.amazon_target_table_dt_mv_backup ON CLUSTER cluster_1S_2R
drop table mis_db.amazon_target_table_dt_mv ON CLUSTER cluster_1S_2R
drop table mis_db.amazon_target_table_dt_mv ON CLUSTER cluster_1S_2R
drop table mis_db.amazon_target_table_dt1 ON CLUSTER cluster_1S_2R

-- mis_db.amazon_target_table_dt definition

CREATE TABLE mis_db.amazon_target_table_dt1
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
    `event_date` DateTime64(6),
    `event_time` DateTime64(6),
    `event_code` String,
    `event_office_facilty_id` String,
    `office_name` String,
    `event_description` String,
    `non_delivery_reason` String
)
ENGINE = ReplicatedReplacingMergeTree()
PRIMARY KEY (article_number, event_date)
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;

----------------old script below
-- mis_db.amazon_target_table_dt_mv source

CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv TO mis_db.amazon_target_table_dt
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

    `event_time` DateTime64(6),

    `event_code` String,

    `event_office_facilty_id` String,

    `office_name` String,

    `event_description` String,

    `non_delivery_reason` String
)
AS SELECT
    cxcm.article_number AS article_number,

    cxcm.article_type AS article_type,

    cxcm.booking_date AS booking_date,

    cxcm.booking_time AS booking_time,

    cxcm.booking_office_facility_id AS booking_office_facility_id,

    cxcm.booking_office_name AS booking_office_name,

    cxcm.booking_pin AS booking_pin,

    cxcm.sender_address_city AS sender_address_city,

    cxcm.destination_office_facility_id AS destination_office_facility_id,

    cxcm.destination_office_name AS destination_office_name,

    cxcm.destination_pincode AS destination_pincode,

    cxcm.destination_city AS destination_city,

    cxcm.destination_country AS destination_country,

    cxcm.receiver_name AS receiver_name,

    cxcm.invoice_no AS invoice_no,

    cxcm.line_item AS line_item,

    cxcm.weight_value AS weight_value,

    cxcm.tariff AS tariff,

    cxcm.cod_amount AS cod_amount,

    cxcm.booking_type AS booking_type,

    cxcm.contract_number AS contract_number,

    cxcm.reference AS reference,

    cxcm.bulk_customer_id AS bulk_customer_id,

    te.event_date AS event_date,

    te.event_date AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
INNER JOIN
(
    SELECT
        t1.article_number,

        t1.event_date,

        t1.event_type,

        t1.event_code,

        t1.office_id,

        t1.office_name,

        t1.delivery_status,

        row_number() OVER (PARTITION BY t1.article_number ORDER BY t1.event_date DESC) AS rn
    FROM mis_db.new_customer_tracking_event_new_mv AS t1
) AS te ON (cxcm.article_number = te.article_number) AND (te.rn = 1)
LEFT JOIN mis_db.customer_log AS el ON cxcm.bulk_customer_id = el.customer_id
WHERE (cxcm.bulk_customer_id != 0) AND (cxcm.booking_date < now());

----
-- mis_db.amazon_target_table_dt_ib_mv source

CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv TO mis_db.amazon_target_table_dt
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

    `event_time` DateTime64(6),

    `event_code` String,

    `event_office_facilty_id` String,

    `office_name` String,

    `event_description` String,

    `non_delivery_reason` String
)
AS 
insert into mis_db.amazon_target_table_dt
SELECT DISTINCT
    cxcm.article_number AS article_number,

    cxcm.article_type AS article_type,

    cxcm.booking_date AS booking_date,

    cxcm.booking_time AS booking_time,

    cxcm.booking_office_facility_id AS booking_office_facility_id,

    cxcm.booking_office_name AS booking_office_name,

    cxcm.booking_pin AS booking_pin,

    cxcm.sender_address_city AS sender_address_city,

    cxcm.destination_office_facility_id AS destination_office_facility_id,

    cxcm.destination_office_name AS destination_office_name,

    cxcm.destination_pincode AS destination_pincode,

    cxcm.destination_city AS destination_city,

    cxcm.destination_country AS destination_country,

    cxcm.receiver_name AS receiver_name,

    cxcm.invoice_no AS invoice_no,

    cxcm.line_item AS line_item,

    cxcm.weight_value AS weight_value,

    cxcm.tariff AS tariff,

    cxcm.cod_amount AS cod_amount,

    cxcm.booking_type AS booking_type,

    cxcm.contract_number AS contract_number,

    cxcm.reference AS reference,

    cxcm.bulk_customer_id AS bulk_customer_id,

    te.event_date AS event_date,

    toDateTime64(te.event_date,
 6) + toIntervalSecond(0) AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
LEFT JOIN
(
    SELECT
        article_number,

        event_date,

        event_code,

        office_id,

        office_name,

        event_type,

        delivery_status
    FROM mis_db.new_customer_tracking_event_new_mv
   
) AS te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el ON cxcm.bulk_customer_id = el.customer_id
WHERE (cxcm.bulk_customer_id != 0) AND (te.article_number IS NOT NULL);