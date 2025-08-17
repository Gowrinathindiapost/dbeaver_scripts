explain ANALYZE 

-- `mis_db`.`new_customer_tracking_event_mv` definition
drop table mis_db.amazon_target_table ON CLUSTER cluster_1S_2R

CREATE TABLE mis_db.amazon_target_table
(
    article_number String,
    article_type String,
    booking_date String,
    booking_time String,
    booking_office_facility_id String,
    booking_office_name String,
    booking_pin Int32,
    sender_address_city String,
    destination_office_facility_id String,
    destination_office_name String,
    destination_pincode Int32,
    destination_city String,
    destination_country String,
    receiver_name String,
    invoice_no String,
    line_item String,
    weight_value Decimal(10, 3),
    tariff Decimal(10, 2),
    cod_amount Decimal(10, 2),
    booking_type String,
    contract_number Int32,
    reference String,
    bulk_customer_id Int64,
    event_date String,
    event_time String,
    event_code String,
    event_office_facilty_id String,
    office_name String,
    event_description String,
    non_delivery_reason String
)
ENGINE = ReplacingMergeTree
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW mis_db.amazon_target_table_mv
ON CLUSTER cluster_1S_2R
TO mis_db.amazon_target_table
AS
insert into mis_db.amazon_target_table
SELECT
    cxcm.article_number, cxcm.article_type,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
    cxcm.booking_office_facility_id,
    cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
    cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
    cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
    cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
    formatDateTime(te.event_date, '%d%m%Y') AS event_date,
    formatDateTime(te.event_date, '%H%i%s') AS event_time,
    te.event_code AS event_code,
    te.office_id AS event_office_facilty_id,
    te.office_name AS office_name,
    te.event_type AS event_description,
    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
INNER JOIN (
    SELECT t1.article_number, t1.event_date, t1.event_type, t1.event_code,
           t1.office_id, t1.office_name, t1.delivery_status
    FROM mis_db.new_customer_tracking_event_mv AS t1
    ANY INNER JOIN (
        SELECT article_number, max(event_date) AS max_event_date
        FROM mis_db.new_customer_tracking_event_mv
        GROUP BY article_number
    ) AS t2
    ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
) te ON cxcm.article_number = te.article_number
WHERE cxcm.bulk_customer_id = 1000002954
  AND parseDateTimeBestEffort(cxcm.booking_date) < now()
  AND te.event_date BETWEEN toDateTime('2025-07-16 00:53:57') AND toDateTime('2025-07-16 23:57:57');



CREATE MATERIALIZED VIEW mis_db.amazon_target_table_mv -- Renamed MV for clarity, or use your preferred MV name
ON CLUSTER cluster_1S_2R
TO mis_db.amazon_target_table -- <--- THIS MUST MATCH YOUR CREATE TABLE NAME
AS
SELECT
	    cxcm.article_number, cxcm.article_type,
	    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
	    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
	    cxcm.booking_office_facility_id,
	    cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
	    cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
	    cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
	    cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
	    formatDateTime((te.event_date), '%d%m%Y') AS event_date,
	    formatDateTime((te.event_date), '%H%i%s') AS event_time,
	    te.event_code AS event_code,
	    te.office_id AS event_office_facilty_id,
	    te.office_name AS office_name,
	    te.event_type AS event_description,
	    te.delivery_status as non_delivery_reason
	FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
	INNER JOIN (
	    SELECT t1.article_number, t1.event_date, t1.event_type, t1.event_code,t1.office_id, t1.office_name, t1.delivery_status
	    FROM mis_db.new_customer_tracking_event_mv AS t1
	    ANY INNER JOIN (
	        SELECT article_number, max(event_date) AS max_event_date
	        FROM mis_db.new_customer_tracking_event_mv
	        GROUP BY article_number
	    ) AS t2
	    ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
	) te ON cxcm.article_number = te.article_number
	LEFT JOIN mis_db.customer_log AS el
	    ON cxcm.bulk_customer_id = el.customer_id
	WHERE cxcm.bulk_customer_id = 1000002954
	    AND parseDateTimeBestEffort(cxcm.booking_date) < now()
	     AND te.event_date BETWEEN
	     parseDateTimeBestEffort('2025-07-16 00:53:57.198516') AND parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
	     
	     
	     
	     insert into mis_db.amazon_target_table
	      SELECT
    cxcm.article_number, cxcm.article_type,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
    cxcm.booking_office_facility_id,
    cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
    cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
    cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
    cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
    formatDateTime(te.event_date, '%d%m%Y') AS event_date,
    formatDateTime(te.event_date, '%H%i%s') AS event_time,
    te.event_code AS event_code,
    te.office_id AS event_office_facilty_id,
    te.office_name AS office_name,
    te.event_type AS event_description,
    te.delivery_status as non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
JOIN (
    SELECT 
        t1.article_number, 
        t1.event_date, 
        t1.event_type, 
        t1.event_code,
        t1.office_id, 
        t1.office_name, 
        t1.delivery_status,
        ROW_NUMBER() OVER (PARTITION BY t1.article_number ORDER BY t1.event_date DESC) AS rn
    FROM mis_db.new_customer_tracking_event_mv AS t1
    WHERE t1.event_date BETWEEN 
        parseDateTimeBestEffort('2025-07-16 00:53:57.198516') AND 
        parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
) te ON cxcm.article_number = te.article_number AND te.rn = 1
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id = 1000002954
    AND parseDateTimeBestEffort(cxcm.booking_date) < now()
    
    
    
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
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
--PARTITION BY toYYYYMM(event_date)  -- Monthly partitioning
PARTITION BY toYearWeek(event_date)
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;


ALTER TABLE mis_db.new_customer_tracking_event_mv
DELETE WHERE event_date < toDateTime('2025-07-07 00:00:00');

SELECT count() FROM mis_db.new_customer_tracking_event_mv
WHERE event_date < toDateTime('2025-07-07 00:00:00');


SELECT count() FROM mis_db.new_customer_tracking_event_mv
WHERE event_date < toDateTime('2025-06-01 00:00:00');

-- Delete in 10 million row chunks (adjust as needed)
ALTER TABLE mis_db.new_customer_tracking_event_mv
DELETE WHERE event_date < toDateTime('2025-06-01 00:00:00');

-- Then the next chunk
ALTER TABLE mis_db.new_customer_tracking_event_mv
DELETE WHERE event_date >= toDateTime('2025-06-01 00:00:00') 
  AND event_date < toDateTime('2025-07-01 00:00:00');



ALTER TABLE mis_db.new_customer_xml_facility_customer_mv
DELETE WHERE parseDateTimeBestEffort(booking_date) < parseDateTimeBestEffort('2025-07-07T00:00:51.198516Z');

SELECT * FROM system.mutations WHERE table = 'new_customer_tracking_event_mv';
SELECT * FROM system.mutations WHERE table = 'new_customer_xml_facility_customer_mv';


SELECT database, table, mutation_id, is_done, latest_failed_part, latest_fail_reason
FROM system.mutations
WHERE table = 'new_customer_tracking_event_mv';

SELECT * FROM system.mutations
WHERE table = 'new_customer_tracking_event_mv';


SELECT * FROM system.merges
WHERE table = 'new_customer_tracking_event_mv';


SELECT table, elapsed, progress, result_part_name
FROM system.merges
WHERE table = 'new_customer_tracking_event_mv';

SELECT mutation_id, is_done, latest_fail_reason
FROM system.mutations
WHERE table = 'new_customer_tracking_event_mv';


KILL MUTATION WHERE table = 'new_customer_tracking_event_mv' AND mutation_id = '0000000001';



WITH
    toDateTime('2025-07-16 00:53:57') AS start_dt,
    toDateTime('2025-07-16 23:57:57') AS end_dt

SELECT
    cxcm.article_number,
    cxcm.article_type,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
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
    formatDateTime(te.event_date, '%d%m%Y') AS event_date,
    formatDateTime(te.event_date, '%H%i%s') AS event_time,
    te.event_code,
    te.office_id AS event_office_facilty_id,
    te.office_name,
    te.event_type AS event_description,
    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
INNER JOIN (
    SELECT *
    FROM (
        SELECT
            article_number,
            event_date,
            event_type,
            event_code,
            office_id,
            office_name,
            delivery_status,
            ROW_NUMBER() OVER (PARTITION BY article_number ORDER BY event_date DESC) AS rn
        FROM mis_db.new_customer_tracking_event_mv
        WHERE event_date BETWEEN start_dt AND end_dt
    )
    WHERE rn = 1
) AS te
    ON cxcm.article_number = te.article_number
WHERE cxcm.bulk_customer_id = 1000002954
  AND parseDateTimeBestEffort(cxcm.booking_date) < now()


