-- `mis_db`.`amazon_target_table_mv` source

drop table mis_db.amazon_target_table_mv ON CLUSTER cluster_1S_2R
optimize TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R;

CREATE MATERIALIZED VIEW mis_db.amazon_target_table_mv ON CLUSTER cluster_1S_2R
TO mis_db.amazon_target_table
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
AS 
insert into mis_db.amazon_target_table
SELECT
    cxcm.article_number,

    cxcm.article_type,

    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date),
 '%d%m%Y') AS booking_date,

    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time),
 '%H%i%s') AS booking_time,

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

    formatDateTime(te.event_date,
 '%d%m%Y') AS event_date,

    formatDateTime(te.event_date,
 '%H%i%s') AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
INNER JOIN
(
    SELECT
        t1.article_number,

        t1.event_date,

        t1.event_type,

        t1.event_code,

        t1.office_id,

        t1.office_name,

        t1.delivery_status
    FROM mis_db.new_customer_tracking_event_mv AS t1
    ANY INNER JOIN
    (
        SELECT
            article_number,

            max(event_date) AS max_event_date
        FROM mis_db.new_customer_tracking_event_mv
        GROUP BY article_number
    ) AS t2 ON (t1.article_number = t2.article_number) AND (t1.event_date = t2.max_event_date)
) AS te ON cxcm.article_number = te.article_number
WHERE (cxcm.bulk_customer_id !=0) 
AND (parseDateTimeBestEffort(cxcm.booking_date) < now()) 



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
  
  drop table mis_db.amazon_target_table_ib_mv ON CLUSTER cluster_1S_2R
  drop table mis_db.amazon_target_table_ib ON CLUSTER cluster_1S_2R
  
  CREATE MATERIALIZED VIEW mis_db.amazon_target_table_ib_mv ON CLUSTER cluster_1S_2R
TO mis_db.amazon_target_table_ib
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
AS 
insert into mis_db.amazon_target_table_ib
SELECT
    cxcm.article_number AS article_number, 
    cxcm.article_type AS article_type,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
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
    formatDateTime(te.event_date, '%d%m%Y') AS event_date,
    formatDateTime(te.event_date, '%H%i%s') AS event_time,
    te.event_code AS event_code,
    te.office_id AS event_office_facilty_id,
    te.office_name AS office_name,
    te.event_type AS event_description,
    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
INNER JOIN mis_db.new_customer_tracking_event_mv te
    ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
    WHERE (cxcm.bulk_customer_id !=0) 
and te.event_code = 'ITEM_BOOK';
