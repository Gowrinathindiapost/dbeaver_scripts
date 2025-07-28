SELECT distinct
    cxcm.article_number, cxcm.article_type,
   formatDateTime(cxcm.booking_date, '%d%m%Y') AS booking_date,
    formatDateTime(cxcm.booking_time, '%H%i%s') AS booking_time,
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
INNER JOIN mis_db.new_customer_tracking_event_mv te
    ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id = 1000002954
AND cxcm.booking_date >= parseDateTime64BestEffort('2025-07-24')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-25')
  AND te.event_date >= parseDateTime64BestEffort('2025-07-24')
  AND te.event_date < parseDateTime64BestEffort('2025-07-25');