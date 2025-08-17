AT150242855IN
INSERT INTO mis_db.customer_master (id, customer_id) VALUES(generateUUIDv4(), 3000068453);
SELECT Distinct
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
WHERE cxcm.bulk_customer_id = 2000008005
  AND cxcm.event_date >= toDateTime64('2025-07-01 00:00:00.000000', 6)
  AND cxcm.event_date < toDateTime64('2025-07-31 23:59:59.000000', 6)
  AND cxcm.event_code in ('ITEM_RETURN','ITEM_REDIRECT')
  
  AND te.event_date BETWEEN parseDateTime64BestEffort('2025-07-27 00:00:00.000000') 
                             AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000')
                             
                             
                             SELECT
    '<?xml version="1.0" encoding="UTF-8"?>' ||
    '<LatestEventDetails>' ||
    arrayStringConcat(
        arrayMap(
            x -> '<ArticleDetails>' ||
                '<ArticleNumber>' || toString(x.1) || '</ArticleNumber>' ||
                '<ArticleType>' || upper(toString(x.2)) || '</ArticleType>' ||
                '<BookingDate>' || toString(x.3) || '</BookingDate>' ||
                '<BookingTime>' || toString(x.4) || '</BookingTime>' ||
                '<BookingOfficeFacilityID>' || upper(toString(x.5)) || '</BookingOfficeFacilityID>' ||
                '<BookingOfficeName>' || upper(toString(x.6)) || '</BookingOfficeName>' ||
                '<BookingPIN>' || toString(x.7) || '</BookingPIN>' ||
                '<SenderAddressCity>' || upper(toString(x.8)) || '</SenderAddressCity>' ||
                '<DestinationOfficeFacilityID>' || upper(toString(x.9)) || '</DestinationOfficeFacilityID>' ||
                '<DestinationOfficeName>' || upper(toString(x.10)) || '</DestinationOfficeName>' ||
                '<DestinationPIN>' || toString(x.11) || '</DestinationPIN>' ||
                '<DestinationCity>' || upper(toString(x.12)) || '</DestinationCity>' ||
                '<DestinationCountry>' || upper(toString(x.13)) || '</DestinationCountry>' ||
                '<ReceiverName>' || upper(replaceRegexpAll(toString(x.14), '[^a-zA-Z0-9 ]', '')) || '</ReceiverName>' ||
                '<InvoiceNo>' || toString(x.15) || '</InvoiceNo>' ||
                '<LineItem/>' ||
                '<WeightValue>' || toString(x.16) || '</WeightValue>' ||
                '<Tariff>' || toString(x.17) || '</Tariff>' ||
                '<CODAmount>' || toString(x.18) || '</CODAmount>' ||
                '<BookingType>' || upper(toString(x.19)) || '</BookingType>' ||
                '<ContractNumber>' || toString(x.20) || '</ContractNumber>' ||
                '<Refrence/>' ||
                '<EventCode>' || upper(toString(x.21)) || '</EventCode>' ||
                '<EventDescription>' || upper(toString(x.22)) || '</EventDescription>' ||
                '<EventOfficeFacilityID>' || upper(toString(x.23)) || '</EventOfficeFacilityID>' ||
                '<EventOfficeName>' || upper(toString(x.24)) || '</EventOfficeName>' ||
                '<EventDate>' || toString(x.25) || '</EventDate>' ||
                '<EventTime>' || toString(x.26) || '</EventTime>' ||
                '<NonDelReason>' || upper(toString(x.27)) || '</NonDelReason>' ||
                '</ArticleDetails>',
            groupArray((
                article_number,
                article_type,
                formatDateTime(booking_date, '%d%m%Y'),
                formatDateTime(booking_time, '%H%i%s'),
                booking_office_facility_id,
                booking_office_name,
                booking_pin,
                sender_address_city,
                destination_office_facility_id,
                destination_office_name,
                destination_pincode,
                destination_city,
                destination_country,
                receiver_name,
                invoice_no,
                weight_value,
                tariff,
                cod_amount,
                booking_type,
                contract_number,
                event_code,
                event_description,
                event_office_facilty_id,
                office_name,
                formatDateTime(event_date, '%d%m%Y'),
                formatDateTime(event_time, '%H%i%s'),
                ifNull(non_delivery_reason, '')
            ))
        )
    ) ||
    '</LatestEventDetails>' AS xml_output
FROM (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY article_number ORDER BY booking_date DESC, booking_time DESC) AS rn
        FROM mis_db.amazon_target_table_dt
        WHERE bulk_customer_id =  3000011691
         AND event_date BETWEEN parseDateTime64BestEffort('2025-07-31 00:00:00.000000') 
                           AND parseDateTime64BestEffort('2025-07-31 23:59:59.000000')
    ) AS ranked
    WHERE rn = 1
) AS filtered_data


WITH max_dates AS (
    SELECT 
        article_number,
        MAX(event_date) AS max_event_date
    FROM mis_db.amazon_target_table_dt
    WHERE bulk_customer_id = 3000011691
      AND event_date >= toDateTime64('2025-07-31 00:00:00.000000', 6)
      AND event_date < toDateTime64('2025-07-31 23:59:59.000000', 6)
    GROUP BY article_number
)
SELECT
    cxcm.article_number, 
    cxcm.article_type,
    formatDateTime(cxcm.booking_date, '%d%m%Y') AS booking_date,
    formatDateTime(cxcm.booking_time, '%H%M%S') AS booking_time,
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
    formatDateTime(cxcm.event_date, '%d%m%Y') AS event_date,
    formatDateTime(cxcm.event_time, '%H%M%S') AS event_time,
    cxcm.event_code AS event_code,
    cxcm.event_office_facilty_id AS event_office_facilty_id,
    cxcm.office_name AS office_name,
    cxcm.event_description AS event_description,
    cxcm.non_delivery_reason AS non_delivery_reason
FROM mis_db.amazon_target_table_dt AS cxcm
JOIN max_dates md ON cxcm.article_number = md.article_number 
                  AND cxcm.event_date = md.max_event_date
WHERE cxcm.bulk_customer_id = 3000011691
  AND cxcm.event_date >= toDateTime64('2025-07-31 00:00:00.000000', 6)
  AND cxcm.event_date < toDateTime64('2025-07-31 23:59:59.000000', 6)