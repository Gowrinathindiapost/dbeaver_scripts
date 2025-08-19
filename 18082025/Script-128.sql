WITH latest_events AS (
    SELECT 
        article_number,
        event_date,
        event_time,
        event_code,
        event_office_facilty_id,
        office_name,
        event_description,
        non_delivery_reason,
        ROW_NUMBER() OVER (PARTITION BY article_number ORDER BY event_date DESC, event_time DESC) AS rn
    FROM mis_db.amazon_target_table_dt
    WHERE bulk_customer_id = 2000011780
      AND article_number IN ('AV155163054IN')
)
SELECT Distinct
    cxcm.article_number, 
    cxcm.article_type,
    formatDateTime(cxcm.booking_date, '%d%m%Y') AS booking_date,
    formatDateTime(cxcm.booking_time, '%H%i%s') AS booking_time,
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
    formatDateTime(le.event_date, '%d%m%Y') AS event_date,
    formatDateTime(le.event_time, '%H%i%s') AS event_time,
    le.event_code,
    le.event_office_facilty_id,
    le.office_name,
    le.event_description,
    le.non_delivery_reason
FROM mis_db.amazon_target_table_dt cxcm
JOIN latest_events le ON cxcm.article_number = le.article_number 
                     AND cxcm.event_date = le.event_date 
                     AND cxcm.event_time = le.event_time
WHERE le.rn = 1
  AND cxcm.bulk_customer_id = 2000011780
  AND cxcm.article_number IN ('AV155163054IN')