SELECT
    DISTINCT cxcm.article_number, 
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
AND cxcm.booking_date >= parseDateTime64BestEffort('2025-07-23 00:01:57.198516')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-24 00:01:57.198516')
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-07-23 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-224 00:01:57.198516')
  AND cxcm.booking_date >= toDateTime64(?, 6)
  AND cxcm.booking_date < toDateTime64(?, 6)
  AND cxcm.event_date >= toDateTime64(?, 6)
  AND cxcm.event_date < toDateTime64(?, 6)
  select distinct(bulk_customer_id) from mis_db.amazon_target_table_dt_ib attdi 
  select * from mis_db.amazon_target_table_dt_ib attdi 
  select distinct(customer_id) from customer_log
  INSERT INTO mis_db.customer_log (id, customer_id, file_name, generation_time, status, error_message, event_date_filter, event_code_filter, generated_article_count) VALUES('40484ec2-e7c1-42f0-9b8a-cd5827e19642', 6000015942, 'D:\\scheduler_tnt\\trackandtrace-scheduler-backend\\DownloadXML\\3000042033_LE_01052025_23072025140015.xml', '2025-07-23 14:00:15.000', 'LE', 'SUCCESS', '2025-05-01', 'LE', 0);
-- mis_db.customer_log definition

select * from mis_db.customer_master
select * from mis_db.customer_log
INSERT INTO mis_db.customer_master (id, customer_id) VALUES(generateUUIDv4(), 3000042033 );
drop table mis_db.customer_master ON CLUSTER cluster_1S_2R
CREATE TABLE mis_db.customer_master ON CLUSTER cluster_1S_2R
(
    `id` UUID DEFAULT generateUUIDv4(),
    `customer_id` Int64,
    `created_date` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (customer_id)
SETTINGS index_granularity = 8192;




