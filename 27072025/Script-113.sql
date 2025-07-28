SELECT
DISTINCT 
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
    formatDateTime(argMax(te.event_date, te.event_date), '%d%m%Y') AS event_date,
    formatDateTime(argMax(te.event_date, te.event_date), '%H%i%s') AS event_time,
    argMax(te.event_code, te.event_date) AS event_code,
    argMax(te.office_id, te.event_date) AS event_office_facilty_id,
    argMax(te.office_name, te.event_date) AS office_name,
    argMax(te.event_type, te.event_date) AS event_description,
    argMax(te.delivery_status, te.event_date) AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
LEFT JOIN mis_db.new_customer_tracking_event_mv AS te
    ON cxcm.article_number = te.article_number
WHERE cxcm.bulk_customer_id = 1000002954
  AND cxcm.booking_date < now()
  AND te.event_date >= parseDateTime64BestEffort('2025-07-25')
  AND te.event_date < parseDateTime64BestEffort('2025-07-26')
GROUP BY
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
    cxcm.bulk_customer_id;