INSERT INTO mis_db.amazon_target_table_dt (
        article_number, article_type, booking_date, booking_time,
        booking_office_facility_id, booking_office_name, booking_pin, sender_address_city,
        destination_office_facility_id, destination_office_name, destination_pincode, 
        destination_city, destination_country, receiver_name, invoice_no, line_item,
        weight_value, tariff, cod_amount, booking_type, contract_number, reference,
        bulk_customer_id, event_date, event_time, event_code, event_office_facilty_id,
        office_name, event_description, non_delivery_reason
    )
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
      te.event_date AS event_date,
    te.event_date AS event_time,
        te.event_code,
        te.office_id AS event_office_facilty_id,
        te.office_name,
        te.event_type AS event_description,
        te.delivery_status AS non_delivery_reason
    FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
    INNER JOIN (
        SELECT 
            t1.article_number, 
            t1.event_date, 
            t1.event_type, 
            t1.event_code,
            t1.office_id, 
            t1.office_name, 
            t1.delivery_status
        FROM mis_db.new_customer_tracking_event_mv AS t1
        INNER JOIN (
            SELECT 
                article_number, 
                max(event_date) AS max_event_date
            FROM mis_db.new_customer_tracking_event_mv
            GROUP BY article_number
        ) AS t2 ON t1.article_number = t2.article_number 
                AND t1.event_date = t2.max_event_date
    ) te ON cxcm.article_number = te.article_number
    LEFT JOIN mis_db.customer_log AS el
        ON cxcm.bulk_customer_id = el.customer_id
    WHERE cxcm.bulk_customer_id != 0
        AND cxcm.booking_date < now()
--        AND te.event_date >= toDateTime64(?, 6)
--  AND te.event_date < toDateTime64(?, 6)
--  
--   AND cxcm.booking_date < now()
    AND te.event_date >= toDateTime64('2025-08-12 00:00:00.000000', 6)
    AND te.event_date < toDateTime64('2025-08-13 00:00:00.000000', 6);