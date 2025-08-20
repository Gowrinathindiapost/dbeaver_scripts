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
--insert into amazon_target_table_dt
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
    WHERE cxcm.bulk_customer_id =  2000014074
        AND cxcm.booking_date < now()
        AND te.event_date >= parseDateTime64BestEffort('2025-08-19 00:00:00.198516')
        AND te.event_date < parseDateTime64BestEffort('2025-08-19 23:59:59.198516')
        --AND te.event_date BETWEEN parseDateTime64BestEffort('2025-07-31 00:00:00.000000') AND parseDateTime64BestEffort('2025-08-09 23:59:59.000000')
      --  AND te.event_code='ITEM_BOOK' --MS009937770IN
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

5000016957--booking, return, redirect 31072025 to 09082025

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
                booking_date,
                booking_time,
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
                event_office_facility_id,
                office_name,
                event_date,
                event_time,
                ifNull(non_delivery_reason, '')
            ))
        )
    ) ||
    '</LatestEventDetails>' AS xml_output
FROM (
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
        cxcm.weight_value, 
        cxcm.tariff, 
        cxcm.cod_amount,
        cxcm.booking_type, 
        cxcm.contract_number, 
        cxcm.bulk_customer_id,
        formatDateTime(te.event_date, '%d%m%Y') AS event_date,
        formatDateTime(toDateTime(te.event_date), '%H%i%s') AS event_time,  -- Extract time from event_date
        te.event_code,
        te.office_id AS event_office_facility_id,
        te.office_name,
        te.event_type AS event_description,
        te.delivery_status AS non_delivery_reason,
        ROW_NUMBER() OVER (PARTITION BY cxcm.article_number ORDER BY te.event_date DESC) AS rn  -- Removed event_time from ordering
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
    WHERE cxcm.bulk_customer_id = 1000002954
        AND cxcm.booking_date < now()
        AND te.event_date BETWEEN parseDateTime64BestEffort('2025-07-31 00:00:00.000000') 
                             AND parseDateTime64BestEffort('2025-08-09 23:59:59.000000')
) AS ranked
WHERE rn = 1

optimize TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R;

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
        WHERE bulk_customer_id =  1000002954
         AND event_date BETWEEN parseDateTime64BestEffort('2025-07-30 00:00:00.000000') 
                           AND parseDateTime64BestEffort('2025-07-30 23:59:59.000000')
    ) AS ranked
    WHERE rn = 1
) AS filtered_data


---testing below
SELECT article_number, COUNT(*) 
FROM (
  -- your full query without ROW_NUMBER filter
) 
GROUP BY article_number 
HAVING COUNT(*) > 1

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
        cxcm.weight_value, 
        cxcm.tariff, 
        cxcm.cod_amount,
        cxcm.booking_type, 
        cxcm.contract_number, 
        cxcm.bulk_customer_id,
        formatDateTime(te.event_date, '%d%m%Y') AS event_date,
        formatDateTime(toDateTime(te.event_date), '%H%i%s') AS event_time,  -- Extract time from event_date
        te.event_code,
        te.office_id AS event_office_facility_id,
        te.office_name,
        te.event_type AS event_description,
        te.delivery_status AS non_delivery_reason,
        ROW_NUMBER() OVER (PARTITION BY cxcm.article_number ORDER BY te.event_date DESC , te.event_type DESC, te.office_id DESC) AS rn  -- Removed event_time from ordering
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
    WHERE cxcm.bulk_customer_id = 1000002954
        AND cxcm.booking_date < now()
        AND te.event_date BETWEEN parseDateTime64BestEffort('2025-07-27 00:00:00.000000') 
                             AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000')
                     ---return RT events        
                             select * from mis_db.new_customer_xml_facility_customer_mv ncxfcnm  
                             left join mis_db.new_customer_tracking_event_mv nctenm on nctenm.article_number =ncxfcnm.article_number 
                             where ncxfcnm.bulk_customer_id =2000000992 and nctenm.event_code IN ('ITEM_RETURN', 'ITEM_REDIRECT')
                             AND nctenm.event_date BETWEEN parseDateTime64BestEffort('2025-04-27 00:00:00.000000') AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000')
                             
SELECT * 
FROM mis_db.new_customer_tracking_event_mv nctenm 
LEFT JOIN mis_db.new_customer_xml_facility_customer_mv ncxfcnm 
  ON nctenm.article_number = ncxfcnm.article_number 
WHERE nctenm.event_code IN ('ITEM_RETURN', 'ITEM_REDIRECT') and ncxfcnm.bulk_customer_id =2000000992


select * from mis_db.tracking_event_mv where article_number='EW265559390IN'

SYSTEM RESTART REPLICA mis_db.new_customer_tracking_event_mv;


SELECT query, type, event_time
FROM system.query_log
WHERE query LIKE '%racking_event_m%'
  AND event_time >= now() - INTERVAL 1 DAY;

SELECT disk_name, path, bytes_on_disk 
FROM system.parts 
WHERE table = 'tracking_event_mv';

SELECT *
FROM system.parts
WHERE table = 'tracking_event_mv'
  AND active;

SELECT
  table,
  sum(bytes_on_disk) / 1024 / 1024 / 1024 AS size_gb
FROM system.parts
WHERE database = 'mis_db'
GROUP BY table;
SHOW CREATE TABLE tracking_event_mv;

SELECT
    toDate(event_date) AS event_day,
    COUNT(DISTINCT article_number) AS article_count
FROM mis_db.tracking_event_mv
GROUP BY event_day
ORDER BY event_day;

SELECT
    toDate(event_date) AS event_day,
    uniqExact(article_number) AS article_count
FROM mis_db.tracking_event_mv
GROUP BY event_day
ORDER BY event_day;

-----------RT files 
select * from mis_db.new_customer_tracking_event_mv
 select * from mis_db.new_customer_xml_facility_customer_mv ncxfcnm  
                             left join mis_db.new_customer_tracking_event_mv nctenm on nctenm.article_number =ncxfcnm.article_number 
                             where ncxfcnm.bulk_customer_id =2000000992 and nctenm.event_code IN ('ITEM_RETURN', 'ITEM_REDIRECT')
                             AND nctenm.event_date BETWEEN parseDateTime64BestEffort('2025-04-27 00:00:00.000000') AND parseDateTime64BestEffort('2025-07-27 23:59:59.000000')
                             
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
                '<EventDescription>' || upper(ifNull(toString(x.22), '')) || '</EventDescription>' ||
                '<EventOfficeFacilityID>' || upper(toString(x.23)) || '</EventOfficeFacilityID>' ||
                '<EventOfficeName>' || upper(toString(x.24)) || '</EventOfficeName>' ||
                '<EventDate>' || toString(x.25) || '</EventDate>' ||
                '<EventTime>' || toString(x.26) || '</EventTime>' ||
                '<NonDelReason>' || upper(toString(x.27)) || '</NonDelReason>' ||
                '</ArticleDetails>',
            groupArray((
                ncxfcnm.article_number,
                ncxfcnm.article_type,
                formatDateTime(ncxfcnm.booking_date, '%d%m%Y'),
                formatDateTime(ncxfcnm.booking_time, '%H%i%s'),
                ncxfcnm.booking_office_facility_id,
                ncxfcnm.booking_office_name,
                ncxfcnm.booking_pin,
                ncxfcnm.sender_address_city,
                ncxfcnm.destination_office_facility_id,
                ncxfcnm.destination_office_name,
                ncxfcnm.destination_pincode,
                ncxfcnm.destination_city,
                ncxfcnm.destination_country,
                ncxfcnm.receiver_name,
                ncxfcnm.invoice_no,
                ncxfcnm.weight_value,
                ncxfcnm.tariff,
                ncxfcnm.cod_amount,
                ncxfcnm.booking_type,
                ncxfcnm.contract_number,
                nctenm.event_code,
                ifNull(nctenm.event_type, ''),  -- Using event_type as event_description
                ifNull(nctenm.office_id, ''),  -- Using office_id as event_office_facilty_id
                ifNull(nctenm.office_name, ''),
                formatDateTime(nctenm.event_date, '%d%m%Y'),
                formatDateTime(nctenm.event_date, '%H%i%s'),
                ifNull(nctenm.delivery_status, '')  -- Using delivery_status as non_delivery_reason
            ))
        )
    ) ||
    '</LatestEventDetails>' AS xml_output
FROM mis_db.new_customer_xml_facility_customer_mv ncxfcnm  
LEFT JOIN mis_db.new_customer_tracking_event_mv nctenm ON nctenm.article_number = ncxfcnm.article_number 
WHERE ncxfcnm.bulk_customer_id = 5000016957 
  AND nctenm.event_code IN ('ITEM_RETURN', 'ITEM_REDIRECT')
  AND nctenm.event_date BETWEEN parseDateTime64BestEffort('2025-07-31 00:00:00.000000') 
                            AND parseDateTime64BestEffort('2025-08-09 23:59:59.000000')
                            
                            
                            
                            select distinct(article_number) from amazon_target_table_dt 