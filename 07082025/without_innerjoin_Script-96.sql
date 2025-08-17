EXPLAIN PLAN 
EXPLAIN PIPELINE
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
    
    
    SELECT
    query_duration_ms,
    read_rows,
    read_bytes,
    result_rows,
    memory_usage,
    ProfileEvents.Names,
    ProfileEvents.Values
FROM system.query_log
WHERE query ILIKE '%new_customer_xml_facility_customer_mv%'
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;


SELECT distinct
    cxcm.article_number, cxcm.article_type,
--    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
--    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
    formatDateTime(cxcm.booking_date, '%d%m%Y') as booking_date,
    formatDateTime(cxcm.booking_time, '%H%i%s') as booking_time,
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
    WHERE t1.event_date BETWEEN 
        parseDateTimeBestEffort('2025-07-16 00:53:57.198516') AND 
        parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
) te ON cxcm.article_number = te.article_number AND te.rn = 1
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id = 1000002954
    AND (cxcm.booking_date) < now()
    
    
    
    ---
    SELECT distinct
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
FROM mis_db.new_customer_xml_facility_customer_mv  AS cxcm 
INNER JOIN mis_db.new_customer_tracking_event_mv  AS te 
    ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id = 1000002954
AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-16 00:53:57.198516') AND 
        parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-16 00:53:57.198516') AND 
        parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
AND te.event_code = 'ITEM_BOOK'

--the below are index for the above
-- Composite index for bulk_customer_id and booking_date filters
ALTER TABLE mis_db.new_customer_xml_facility_customer_mv
ADD INDEX idx_cxcm_customer_date (bulk_customer_id, parseDateTimeBestEffort(booking_date))
TYPE minmax GRANULARITY 3;

-- Index for article_number used in JOIN
ALTER TABLE mis_db.new_customer_xml_facility_customer_mv
ADD INDEX idx_cxcm_article (article_number) TYPE bloom_filter GRANULARITY 4;

-- Composite index for JOIN condition and event filters
ALTER TABLE mis_db.new_customer_tracking_event_mv
ADD INDEX idx_te_article_date_code (article_number, event_date, event_code)
TYPE minmax GRANULARITY 3;

-- Additional index for event_date range queries
ALTER TABLE mis_db.new_customer_tracking_event_mv
ADD INDEX idx_te_date_code (event_date, event_code) TYPE minmax GRANULARITY 3;

ALTER TABLE mis_db.customer_log
ADD INDEX idx_log_customer (customer_id) TYPE bloom_filter GRANULARITY 4;

ALTER TABLE mis_db.new_customer_xml_facility_customer_mv
ADD COLUMN booking_date_parsed DateTime MATERIALIZED parseDateTimeBestEffort(booking_date);

ALTER TABLE mis_db.new_customer_tracking_event_mv
ADD COLUMN event_date_parsed DateTime MATERIALIZED parseDateTimeBestEffort(event_date);


 SELECT distinct
    cxcm.article_number, cxcm.article_type,
--    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
--    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
    formatDateTime(cxcm.booking_date, '%d%m%Y') as booking_date,
    formatDateTime(cxcm.booking_time, '%H%i%s') as booking_time,
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
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm FINAL
INNER JOIN mis_db.new_customer_tracking_event_new_mv te FINAL
    ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id = 1000002954
AND (cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-16 00:53:57.198516') AND 
        parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-16 00:53:57.198516') AND 
        parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
AND te.event_code = 'ITEM_BOOK'



--- deep below working


SELECT DISTINCT
    cxcm.article_number, 
    cxcm.article_type,
    formatDateTime(cxcm.booking_date, '%d%m%Y') as booking_date,
    formatDateTime(cxcm.booking_time, '%H%i%s') as booking_time,
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
    te.event_code AS event_code,
    te.office_id AS event_office_facilty_id,
    te.office_name AS office_name,
    te.event_type AS event_description,
    te.delivery_status as non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm FINAL
LEFT JOIN (
    SELECT 
        article_number,
        event_date,
        event_code,
        office_id,
        office_name,
        event_type,
        delivery_status
    FROM mis_db.new_customer_tracking_event_new_mv FINAL
    WHERE event_date BETWEEN parseDateTimeBestEffort('2025-07-16 00:53:57.198516') 
                        AND parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
      AND event_code = 'ITEM_BOOK'
) AS te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id = 1000002954
  AND cxcm.booking_date BETWEEN parseDateTimeBestEffort('2025-07-16 00:53:57.198516') 
                          AND parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
  AND te.article_number IS NOT NULL  -- This effectively makes it an INNER JOIN
  
---deep
--SELECT DISTINCT
--    cxcm.article_number, 
--    cxcm.article_type,
--    formatDateTime(cxcm.booking_date, '%d%m%Y') as booking_date,
--    formatDateTime(cxcm.booking_time, '%H%i%s') as booking_time,
--    cxcm.booking_office_facility_id,
--    cxcm.booking_office_name, 
--    cxcm.booking_pin, 
--    cxcm.sender_address_city, 
--    cxcm.destination_office_facility_id,
--    cxcm.destination_office_name, 
--    cxcm.destination_pincode, 
--    cxcm.destination_city, 
--    cxcm.destination_country,
--    cxcm.receiver_name, 
--    cxcm.invoice_no, 
--    cxcm.line_item, 
--    cxcm.weight_value, 
--    cxcm.tariff, 
--    cxcm.cod_amount,
--    cxcm.booking_type, 
--    cxcm.contract_number, 
--    cxcm.reference, 
--    cxcm.bulk_customer_id,
--    formatDateTime(te.event_date, '%d%m%Y') AS event_date,
--    formatDateTime(te.event_date, '%H%i%s') AS event_time,
--    te.event_code AS event_code,
--    te.office_id AS event_office_facilty_id,
--    te.office_name AS office_name,
--    te.event_type AS event_description,
--    te.delivery_status as non_delivery_reason
--FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm FINAL
--ARRAY JOIN arrayFilter(
--    x -> x.event_date BETWEEN parseDateTimeBestEffort('2025-07-16 00:53:57.198516') 
--                          AND parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
--           AND x.event_code = 'ITEM_BOOK',
--    arrayConcat([null], (SELECT groupArray(t) 
--                         FROM mis_db.new_customer_tracking_event_new_mv t FINAL
--                         WHERE t.article_number = cxcm.article_number))
--) AS te
--LEFT JOIN mis_db.customer_log AS el
--    ON cxcm.bulk_customer_id = el.customer_id
--WHERE cxcm.bulk_customer_id = 1000002954
--  AND cxcm.booking_date BETWEEN parseDateTimeBestEffort('2025-07-16 00:53:57.198516') 
--                          AND parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
  
  
  -- Composite index for bulk_customer_id and booking_date range queries
ALTER TABLE mis_db.new_customer_xml_facility_customer_new_mv 
ADD INDEX idx_cxcm_customer_booking (bulk_customer_id, booking_date) TYPE minmax GRANULARITY 3;

-- Index for article_number used in JOIN
ALTER TABLE mis_db.new_customer_xml_facility_customer_new_mv
ADD INDEX idx_cxcm_article (article_number) TYPE bloom_filter GRANULARITY 4;


-- Composite index for event_date range and event_code filter
ALTER TABLE mis_db.new_customer_tracking_event_new_mv
ADD INDEX idx_te_date_code (event_date, event_code) TYPE minmax GRANULARITY 3;

-- Index for article_number used in JOIN
ALTER TABLE mis_db.new_customer_tracking_event_new_mv
ADD INDEX idx_te_article (article_number) TYPE bloom_filter GRANULARITY 4;


ALTER TABLE mis_db.customer_log
ADD INDEX idx_log_customer (customer_id) TYPE bloom_filter GRANULARITY 4;

SHOW INDEX FROM mis_db.new_customer_xml_facility_customer_new_mv;

