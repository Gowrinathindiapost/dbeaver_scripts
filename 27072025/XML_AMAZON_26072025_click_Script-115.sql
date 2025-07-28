SELECT
	'<?xml version="1.0" encoding="UTF-8"?>'||
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
                cxcm.weight_value,
                cxcm.tariff,
                cxcm.cod_amount,
                cxcm.booking_type,
                cxcm.contract_number,
                cxcm.event_code,
                cxcm.event_description,
                cxcm.event_office_facilty_id,
                cxcm.office_name,
                cxcm.event_date,
                cxcm.event_time,
                ifNull(cxcm.non_delivery_reason, '')
            ))
        )
    ) ||
    '</LatestEventDetails>' AS xml_output
FROM (
insert into amazon_target_table_dt
    SELECT DISTINCT
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
        formatDateTime(te.event_date, '%d%m%Y') AS event_date,
        formatDateTime(te.event_date, '%H%i%s') AS event_time,
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
    WHERE cxcm.bulk_customer_id =  3000042436
        AND cxcm.booking_date < now()
--        AND te.event_date >= parseDateTime64BestEffort('2025-07-26 00:00:57.198516')
--        AND te.event_date < parseDateTime64BestEffort('2025-07-27 00:00:15.198516')
        AND te.event_date BETWEEN parseDateTime64BestEffort('2025-07-26 00:00:00.000000') AND parseDateTime64BestEffort('2025-07-26 23:59:59.000000')
       -- AND te.event_code='ITEM_BOOK'
) AS cxcm;
 WHERE event_date > parseDateTime64BestEffort('2025-07-25 00:24:53.952')
      AND event_date <= parseDateTime64BestEffort('2025-07-26 09:24:53.952')
      AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-16 00:01:57.198516')
      
--SELECT
--    replaceRegexpAll(your_column, '[^a-zA-Z0-9 ]', '') AS cleaned_column
--FROM your_table;
--
--'<ReceiverName>' || upper(replaceRegexpAll(toString(x.14), '[^a-zA-Z0-9 ]', '')) || '</ReceiverName>' ||

--old below
'<ReceiverName>' || upper(toString(x.14)) || '</ReceiverName>' ||

SELECT
    replaceRegexpAll(your_column, '[^a-zA-Z0-9 ]', '') AS cleaned_column
FROM your_table;


@919176641355 sir files generated for the below customers
1000002954
2000000992
2000008005
2000014074
2000014100: Nil files
3000042033

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
WHERE cxcm.bulk_customer_id = 3000042033
  AND cxcm.event_date >= toDateTime64('1970-09-01 00:00:00.000000', 6)
  AND cxcm.event_date < toDateTime64('1970-09-30 23:59:59.999999', 6)
  
  
  
  SELECT DISTINCT
        cxcm.article_number, 
        cxcm.article_type,
        cxcm.booking_date,
        cxcm.booking_time ,
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
        te.event_date as event_time,
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
    WHERE cxcm.bulk_customer_id =  3000042436
        AND cxcm.booking_date < now()
        AND te.event_date BETWEEN parseDateTime64BestEffort('2025-07-26 00:00:00.000000') AND parseDateTime64BestEffort('2025-07-26 23:59:59.000000')
        
        
        WITH max_dates AS (
    SELECT 
        article_number,
        MAX(event_date) AS max_event_date
    FROM mis_db.amazon_target_table_dt
    WHERE bulk_customer_id = 3000042436
      AND event_date >= toDateTime64('1970-07-01 00:00:00.000000', 6)
      AND event_date < toDateTime64('1970-11-26 00:00:00.000000', 6)
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
WHERE cxcm.bulk_customer_id = 3000042436
  AND cxcm.event_date >= toDateTime64('1970-07-06 00:00:00.000000', 6)
  AND cxcm.event_date < toDateTime64('1970-11-26 00:00:00.000000', 6)