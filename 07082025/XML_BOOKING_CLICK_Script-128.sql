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
FROM mis_db.amazon_target_table_dt_ib
WHERE bulk_customer_id = 1000002954
AND booking_date BETWEEN parseDateTime64BestEffort('2025-08-03 00:00:00.000000') 
                     AND parseDateTime64BestEffort('2025-08-03 23:59:59.000000')
                     select distinct * 
                     FROM mis_db.amazon_target_table_dt_ib
WHERE bulk_customer_id = 1000002954
AND booking_date BETWEEN parseDateTime64BestEffort('2025-07-27 00:00:00.000000') 
                     AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000') AND
                     event_date BETWEEN parseDateTime64BestEffort('2025-07-27 00:00:00.000000') 
                     AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000')
                     
                     
                     SELECT DISTINCT *
FROM mis_db.amazon_target_table_dt_ib
WHERE bulk_customer_id = 1000002954
AND booking_date = (
    SELECT MAX(booking_date) 
    FROM mis_db.amazon_target_table_dt_ib 
    WHERE bulk_customer_id = 1000002954
    AND booking_date BETWEEN parseDateTime64BestEffort('2025-07-27 00:00:00.000000') 
                         AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000')
)

 SELECT *
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY article_number ORDER BY booking_date DESC, booking_time DESC) AS rn
    FROM mis_db.amazon_target_table_dt_ib
    WHERE bulk_customer_id = 1000002954
    AND booking_date BETWEEN parseDateTime64BestEffort('2025-07-27 00:00:00.000000') 
                         AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000')
                         AND
                     event_date BETWEEN parseDateTime64BestEffort('2025-07-27 00:00:00.000000') 
                     AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000')
) AS ranked
WHERE rn = 1

SELECT *
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY article_number ORDER BY booking_date DESC, booking_time DESC) AS rn
    FROM mis_db.amazon_target_table_dt_ib
    WHERE bulk_customer_id = 1000002954
    AND booking_date BETWEEN parseDateTime64BestEffort('2025-07-27 00:00:00.000000') 
                         AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000')
    AND event_date BETWEEN parseDateTime64BestEffort('2025-07-27 00:00:00.000000') 
                       AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000')
) AS ranked
WHERE rn = 1


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
        FROM mis_db.amazon_target_table_dt_ib
        WHERE bulk_customer_id = 2000008005
        AND booking_date BETWEEN parseDateTime64BestEffort('2025-08-03 00:00:00.000000') 
                             AND parseDateTime64BestEffort('2025-08-03 23:59:59.000000')
        AND event_date BETWEEN parseDateTime64BestEffort('2025-08-03 00:00:00.000000') 
                           AND parseDateTime64BestEffort('2025-08-03 23:59:59.000000')
    ) AS ranked
    WHERE rn = 1
) AS filtered_data