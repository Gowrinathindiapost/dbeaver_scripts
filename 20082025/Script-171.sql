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
    toDateTime64(te.event_date, 6) + toIntervalSecond(0) AS event_time,
    te.event_code AS event_code,
    te.office_id AS event_office_facilty_id,
    te.office_name AS office_name,
    te.event_type AS event_description,
    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
INNER JOIN (
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
LEFT JOIN mis_db.customer_log AS el ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id != 0
