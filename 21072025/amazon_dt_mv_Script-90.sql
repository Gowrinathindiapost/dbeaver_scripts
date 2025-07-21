-- `mis_db`.`amazon_target_table_mv` source

drop table mis_db.amazon_target_table_dt_mv ON CLUSTER cluster_1S_2R
optimize TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R;

CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv ON CLUSTER cluster_1S_2R
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
    (cxcm.booking_date) AS booking_date,
    (cxcm.booking_time) AS booking_time,
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
    FROM mis_db.new_customer_tracking_event_new_mv AS t1
) te ON cxcm.article_number = te.article_number AND te.rn = 1
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id != 0
AND (cxcm.booking_date) < now();
--CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv ON CLUSTER cluster_1S_2R
--TO mis_db.amazon_target_table
--(
--
--    `article_number` String,
--
--    `article_type` String,
--
--    `booking_date` DateTime64(6),
--
--
--    `booking_time` DateTime64(6),
--
--
--    `booking_office_facility_id` String,
--
--    `booking_office_name` String,
--
--    `booking_pin` Int32,
--
--    `sender_address_city` String,
--
--    `destination_office_facility_id` String,
--
--    `destination_office_name` String,
--
--    `destination_pincode` Int32,
--
--    `destination_city` String,
--
--    `destination_country` String,
--
--    `receiver_name` String,
--
--    `invoice_no` String,
--
--    `line_item` String,
--
--    `weight_value` Decimal(10,
-- 3),
--
--    `tariff` Decimal(10,
-- 2),
--
--    `cod_amount` Decimal(10,
-- 2),
--
--    `booking_type` String,
--
--    `contract_number` Int32,
--
--    `reference` String,
--
--    `bulk_customer_id` Int64,
--
--    `event_date` DateTime64(6),
--
--
--    `event_time` DateTime64(6),
--
--
--    `event_code` String,
--
--    `event_office_facilty_id` String,
--
--    `office_name` String,
--
--    `event_description` String,
--
--    `non_delivery_reason` String
--)
--AS 
----insert into mis_db.amazon_target_table
--SELECT
--    cxcm.article_number, cxcm.article_type,
--    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
--    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
--    cxcm.booking_office_facility_id,
--    cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
--    cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
--    cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
--    cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
--    formatDateTime(te.event_date, '%d%m%Y') AS event_date,
--    formatDateTime(te.event_date, '%H%i%s') AS event_time,
--    te.event_code AS event_code,
--    te.office_id AS event_office_facilty_id,
--    te.office_name AS office_name,
--    te.event_type AS event_description,
--    te.delivery_status as non_delivery_reason
--FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
--JOIN (
--    SELECT 
--        t1.article_number, 
--        t1.event_date, 
--        t1.event_type, 
--        t1.event_code,
--        t1.office_id, 
--        t1.office_name, 
--        t1.delivery_status,
--        ROW_NUMBER() OVER (PARTITION BY t1.article_number ORDER BY t1.event_date DESC) AS rn
--    FROM mis_db.new_customer_tracking_event_mv AS t1
--    WHERE t1.event_date BETWEEN 
--        parseDateTimeBestEffort('2025-07-16 00:53:57.198516') AND 
--        parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
--) te ON cxcm.article_number = te.article_number AND te.rn = 1
--LEFT JOIN mis_db.customer_log AS el
--    ON cxcm.bulk_customer_id = el.customer_id
--WHERE cxcm.bulk_customer_id !=0
--    AND parseDateTimeBestEffort(cxcm.booking_date) < now()




SELECT *
FROM mis_db.amazon_target_table te
WHERE booking_date LIKE '%07-202%';
SELECT DISTINCT booking_date
FROM mis_db.amazon_target_table
WHERE booking_date LIKE '%707%';


SELECT *
FROM mis_db.amazon_target_table te --where booking_date like %707-2025%
WHERE te.bulk_customer_id = 1000002954
  AND parseDateTimeBestEffort(te.booking_date) < now();

AND ((te.event_date >= toDateTime('2025-07-16 00:53:57')) 
AND (te.event_date <= toDateTime('2025-07-16 23:57:57')));

SELECT *
FROM mis_db.amazon_target_table te
WHERE te.bulk_customer_id = 1000002954
  AND parseDateTimeBestEffort(
        concat(
            substring(te.booking_date, 5, 4), '-',  -- year
            substring(te.booking_date, 3, 2), '-',  -- month
            substring(te.booking_date, 1, 2)        -- day
        )
      ) < now()
  AND parseDateTimeBestEffort(
        concat(
            substring(te.event_date, 5, 4), '-', 
            substring(te.event_date, 3, 2), '-', 
            substring(te.event_date, 1, 2)
        )
      ) BETWEEN toDateTime('2025-07-16 00:53:57')
          AND toDateTime('2025-07-16 23:57:57');

SELECT DISTINCT booking_date
FROM mis_db.amazon_target_table
WHERE booking_date LIKE '%707%';

SELECT *,
  parseDateTimeBestEffort(
    concat(
      substring(booking_date, 5, 4), '-',  -- year
      substring(booking_date, 3, 2), '-',  -- month
      substring(booking_date, 1, 2)       -- day
    )
  ) AS parsed_booking_date
FROM mis_db.amazon_target_table
WHERE
  match(booking_date, '^[0-3][0-9][0-1][0-9][2-3][0-9]{3}$');  -- Safe DDMMYYYY
  
  SELECT *
FROM mis_db.amazon_target_table 
WHERE booking_date >= '16072025'
  AND booking_date < now() and  `bulk_customer_id` =1000002954  and event_code='ITEM_BOOK' and  article_number='EZ771930502IN'
  and event_date>='16072025' and  event_date <= '17072025'

  SELECT *
FROM mis_db.amazon_target_table 
WHERE booking_date >= '16072025'
  AND booking_date < formatDateTime(now(), '%d%m%Y')
  SELECT *
FROM mis_db.amazon_target_table_ib 
WHERE booking_date >= '16072025'
  AND booking_date < formatDateTime(now(), '%d%m%Y') and bulk_customer_id=1000002954
  SELECT *
FROM mis_db.amazon_target_table_dt 
WHERE booking_date >= '16072025'
  AND booking_date < formatDateTime(now(), '%d%m%Y')
  SELECT *
FROM mis_db.amazon_target_table_ib 
WHERE booking_date >= '16072025'
  AND booking_date < formatDateTime(now(), '%d%m%Y') and bulk_customer_id=1000002954
  SELECT *
--    article_number,
--    article_type,
--    booking_date,
--    booking_time,
--    formatDateTime(booking_date, '%d%m%Y') AS booking_date1,
--    formatDateTime(booking_time, '%H%i%s') AS booking_time1
    -- Rest of the columns as above
FROM mis_db.amazon_target_table_dt_ib where booking_date >= todatetime('2025-07-02T00:53:57.198516Z',6)
LIMIT 100;  -- Adjust limit as needed

SELECT *
--    article_number,
--    article_type,
--    formatDateTime(booking_date, '%d%m%Y') AS formatted_booking_date,
--    formatDateTime(booking_time, '%H%i%s') AS formatted_booking_time
    -- Other columns as needed
FROM mis_db.amazon_target_table_dt_ib
WHERE booking_date >= toDateTime64('2025-07-16 00:00:00.000000', 6)
  AND booking_date < toDateTime64('2025-07-17 00:00:00.000000', 6) and bulk_customer_id='1000002954'
ORDER BY booking_date DESC;
  
  drop table mis_db.amazon_target_table_dt_ib_mv ON CLUSTER cluster_1S_2R
  drop table mis_db.amazon_target_table_dt_ib ON CLUSTER cluster_1S_2R
  
  CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_ib_mv ON CLUSTER cluster_1S_2R
TO mis_db.amazon_target_table_dt_ib
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
insert into mis_db.amazon_target_table_dt_ib
SELECT DISTINCT
    cxcm.article_number AS article_number, 
    cxcm.article_type AS article_type,
--    formatDateTime(cxcm.booking_date, '%d%m%Y') AS booking_date,
--    formatDateTime(cxcm.booking_time, '%H%i%s') AS booking_time,
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
--    formatDateTime(te.event_date, '%d%m%Y') AS event_date,
--    formatDateTime(te.event_date, '%H%i%s') AS event_time,
    te.event_date as event_date,
    te.event_date as event_date,
    te.event_code AS event_code,
    te.office_id AS event_office_facilty_id,
    te.office_name AS office_name,
    te.event_type AS event_description,
    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
LEFT JOIN (
    SELECT 
        article_number,
        event_date,
        event_code,
        office_id,
        office_name,
        event_type,
        delivery_status
    FROM mis_db.new_customer_tracking_event_new_mv
    WHERE event_code = 'ITEM_BOOK'
) AS te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id != 0
AND te.article_number IS NOT NULL;



SELECT
    *
FROM mis_db.amazon_target_table_dt AS cxcm
WHERE cxcm.bulk_customer_id = 1000002954
  AND cxcm.event_date >= toDateTime64('2025-07-16 00:00:00.000000', 6)
  AND cxcm.event_date < toDateTime64('2025-07-17 00:00:00.000000', 6)


SELECT
    cxcm.article_number, 
    cxcm.article_type,
    formatDateTime(cxcm.booking_date, '%d%m%Y') AS booking_date,
    formatDateTime(cxcm.booking_time, '%H%i%s') AS booking_time,
    cxcm.booking_office_facility_id,
    cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city,
    cxcm.destination_office_facility_id,
    cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
    cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
    cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
    formatDateTime(cxcm.event_date, '%d%m%Y') AS event_date,
    formatDateTime(cxcm.event_time, '%H%i%s') AS event_time,
    cxcm.event_code AS event_code,
    cxcm.event_office_facilty_id AS event_office_facilty_id,
    cxcm.office_name AS office_name,
    cxcm.event_description AS event_description,
    cxcm.non_delivery_reason AS non_delivery_reason
FROM mis_db.amazon_target_table_dt AS cxcm
WHERE cxcm.bulk_customer_id = 1000002954
  AND cxcm.event_date >= toDateTime64('2025-07-16 00:00:00.000000', 6)
  AND cxcm.event_date < toDateTime64('2025-07-17 00:00:00.000000', 6)
