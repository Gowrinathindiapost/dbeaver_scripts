

optimize TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R;
RENAME TABLE new_customer_tracking_event_mv TO new_customer_tracking_event_mv_old, 
new_customer_tracking_event_new_mv TO new_customer_tracking_event_mv;

SELECT 
    partition, 
    count() AS part_count
FROM system.parts
WHERE 
    database = 'mis_db'
    AND table = 'new_customer_tracking_event_mv'
    AND active
GROUP BY partition
ORDER BY partition;


SELECT 
    partition, 
    min(min_date) as from_date,
    max(max_date) as to_date,
    count() AS part_count
FROM system.parts
WHERE 
    database = 'mis_db' 
    AND table = 'new_customer_tracking_event_mv'
    AND active
GROUP BY partition
ORDER BY partition;

SHOW CREATE TABLE mis_db.amazon_target_table_dt_ib;



RENAME TABLE new_customer_xml_facility_customer_mv TO new_customer_xml_facility_customer_mv_old, 
new_customer_xml_facility_customer_new_mv TO new_customer_tracking_event_mv;


optimize TABLE mis_db.amazon_target_table_dt ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.amazon_target_table_dt_ib ON CLUSTER cluster_1S_2R;

OPTIMIZE TABLE mis_db.amazon_target_table_dt FINAL PARTITION '2025-07-01' ON CLUSTER cluster_1S_2R;
OPTIMIZE TABLE mis_db.amazon_target_table_dt_ib FINAL PARTITION '2025-07-01' ON CLUSTER cluster_1S_2R;

SELECT host_name, error FROM system.replication_queue WHERE error != '';


OPTIMIZE TABLE mis_db.amazon_target_table_dt FINAL;

SELECT * FROM system.merges WHERE table = 'amazon_target_table_dt';

SELECT *
FROM system.merges
WHERE table = 'new_customer_tracking_event_mv'
  AND database = 'mis_db';

SELECT partition, count() AS parts
FROM system.parts
WHERE table = 'new_customer_tracking_event_mv'
  AND database = 'mis_db'
  AND active
GROUP BY partition
ORDER BY parts DESC;

SELECT 
    '<LatestEventDetails><ArticleDetails>' ||
    '<ArticleNumber>' || toString(cxcm.article_number) || '</ArticleNumber>' ||
    '<ArticleType>' || toString(cxcm.article_type) || '</ArticleType>' ||
    '<BookingDate>' || formatDateTime(cxcm.booking_date, '%d%m%Y') || '</BookingDate>' ||
    '<BookingTime>' || formatDateTime(cxcm.booking_time, '%H%i%s') || '</BookingTime>' ||
    '<BookingOfficeFacilityID>' || toString(cxcm.booking_office_facility_id) || '</BookingOfficeFacilityID>' ||
    '<BookingOfficeName>' || toString(cxcm.booking_office_name) || '</BookingOfficeName>' ||
    '<BookingPIN>' || toString(cxcm.booking_pin) || '</BookingPIN>' ||
    '<SenderAddressCity>' || toString(cxcm.sender_address_city) || '</SenderAddressCity>' ||
    '<DestinationOfficeFacilityID>' || toString(cxcm.destination_office_facility_id) || '</DestinationOfficeFacilityID>' ||
    '<DestinationOfficeName>' || toString(cxcm.destination_office_name) || '</DestinationOfficeName>' ||
    '<DestinationPIN>' || toString(cxcm.destination_pincode) || '</DestinationPIN>' ||
    '<DestinationCity>' || toString(cxcm.destination_city) || '</DestinationCity>' ||
    '<DestinationCountry>' || toString(cxcm.destination_country) || '</DestinationCountry>' ||
    '<ReceiverName>' || toString(cxcm.receiver_name) || '</ReceiverName>' ||
    '<InvoiceNo>' || toString(cxcm.invoice_no) || '</InvoiceNo>' ||
    '<LineItem/>' ||
    '<WeightValue>' || toString(cxcm.weight_value) || '</WeightValue>' ||
    '<Tariff>' || toString(cxcm.tariff) || '</Tariff>' ||
    '<CODAmount>' || toString(cxcm.cod_amount) || '</CODAmount>' ||
    '<BookingType>' || toString(cxcm.booking_type) || '</BookingType>' ||
    '<ContractNumber>' || toString(cxcm.contract_number) || '</ContractNumber>' ||
    '<Refrence/>' ||
    '<EventCode>' || toString(cxcm.event_code) || '</EventCode>' ||
    '<EventDescription>' || toString(cxcm.event_description) || '</EventDescription>' ||
    '<EventOfficeFacilityID>' || toString(cxcm.event_office_facilty_id) || '</EventOfficeFacilityID>' ||
    '<EventOfficeName>' || toString(cxcm.office_name) || '</EventOfficeName>' ||
    '<EventDate>' || formatDateTime(cxcm.event_date, '%d%m%Y') || '</EventDate>' ||
    '<EventTime>' || formatDateTime(cxcm.event_time, '%H%i%s') || '</EventTime>' ||
    '<NonDelReason>' || ifNull(toString(cxcm.non_delivery_reason), '') || '</NonDelReason>' ||
    '</ArticleDetails></LatestEventDetails>' AS xml_output
FROM mis_db.amazon_target_table_dt_ib AS cxcm
WHERE cxcm.bulk_customer_id = 1000002954
  AND cxcm.booking_date >= parseDateTime64BestEffort('2025-07-17 00:01:57.198516')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-18 00:01:57.198516')
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-07-17 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-18 00:01:57.198516')

  SELECT *
FROM mis_db.amazon_target_table_dt
WHERE toDate(event_date) = toDate(now() - INTERVAL 1 DAY)
FORMAT XML;


SELECT 
    '<LatestEventDetails>' || 
    groupArray(
        '<ArticleDetails>' ||
        '<ArticleNumber>' || toString(cxcm.article_number) || '</ArticleNumber>' ||
        '<ArticleType>' || toString(cxcm.article_type) || '</ArticleType>' ||
        '<BookingDate>' || formatDateTime(cxcm.booking_date, '%d%m%Y') || '</BookingDate>' ||
        '<BookingTime>' || formatDateTime(cxcm.booking_time, '%H%i%s') || '</BookingTime>' ||
        '<BookingOfficeFacilityID>' || toString(cxcm.booking_office_facility_id) || '</BookingOfficeFacilityID>' ||
        '<BookingOfficeName>' || toString(cxcm.booking_office_name) || '</BookingOfficeName>' ||
        '<BookingPIN>' || toString(cxcm.booking_pin) || '</BookingPIN>' ||
        '<SenderAddressCity>' || toString(cxcm.sender_address_city) || '</SenderAddressCity>' ||
        '<DestinationOfficeFacilityID>' || toString(cxcm.destination_office_facility_id) || '</DestinationOfficeFacilityID>' ||
        '<DestinationOfficeName>' || toString(cxcm.destination_office_name) || '</DestinationOfficeName>' ||
        '<DestinationPIN>' || toString(cxcm.destination_pincode) || '</DestinationPIN>' ||
        '<DestinationCity>' || toString(cxcm.destination_city) || '</DestinationCity>' ||
        '<DestinationCountry>' || toString(cxcm.destination_country) || '</DestinationCountry>' ||
        '<ReceiverName>' || toString(cxcm.receiver_name) || '</ReceiverName>' ||
        '<InvoiceNo>' || toString(cxcm.invoice_no) || '</InvoiceNo>' ||
        '<LineItem/>' ||
        '<WeightValue>' || toString(cxcm.weight_value) || '</WeightValue>' ||
        '<Tariff>' || toString(cxcm.tariff) || '</Tariff>' ||
        '<CODAmount>' || toString(cxcm.cod_amount) || '</CODAmount>' ||
        '<BookingType>' || toString(cxcm.booking_type) || '</BookingType>' ||
        '<ContractNumber>' || toString(cxcm.contract_number) || '</ContractNumber>' ||
        '<Refrence/>' ||
        '<EventCode>' || toString(cxcm.event_code) || '</EventCode>' ||
        '<EventDescription>' || toString(cxcm.event_description) || '</EventDescription>' ||
        '<EventOfficeFacilityID>' || toString(cxcm.event_office_facilty_id) || '</EventOfficeFacilityID>' ||
        '<EventOfficeName>' || toString(cxcm.office_name) || '</EventOfficeName>' ||
        '<EventDate>' || formatDateTime(cxcm.event_date, '%d%m%Y') || '</EventDate>' ||
        '<EventTime>' || formatDateTime(cxcm.event_time, '%H%i%s') || '</EventTime>' ||
        '<NonDelReason>' || ifNull(toString(cxcm.non_delivery_reason), '') || '</NonDelReason>' ||
        '</ArticleDetails>'
    ) || 
    '</LatestEventDetails>' AS xml_output
FROM mis_db.amazon_target_table_dt_ib AS cxcm
WHERE cxcm.bulk_customer_id = 1000002954
  AND cxcm.booking_date >= parseDateTime64BestEffort('2025-07-17 00:01:57.198516')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-18 00:01:57.198516')
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-07-17 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-18 00:01:57.198516')
GROUP BY 1=1




SELECT 
    '<LatestEventDetails>' || 
    arrayStringConcat(
        arrayMap(
            x -> '<ArticleDetails>' ||
                '<ArticleNumber>' || toString(x.1) || '</ArticleNumber>' ||
                '<ArticleType>' || toString(x.2) || '</ArticleType>' ||
                '<BookingDate>' || toString(x.3) || '</BookingDate>' ||
                '<BookingTime>' || toString(x.4) || '</BookingTime>' ||
                '<BookingOfficeFacilityID>' || toString(x.5) || '</BookingOfficeFacilityID>' ||
                '<BookingOfficeName>' || toString(x.6) || '</BookingOfficeName>' ||
                '<BookingPIN>' || toString(x.7) || '</BookingPIN>' ||
                '<SenderAddressCity>' || toString(x.8) || '</SenderAddressCity>' ||
                '<DestinationOfficeFacilityID>' || toString(x.9) || '</DestinationOfficeFacilityID>' ||
                '<DestinationOfficeName>' || toString(x.10) || '</DestinationOfficeName>' ||
                '<DestinationPIN>' || toString(x.11) || '</DestinationPIN>' ||
                '<DestinationCity>' || toString(x.12) || '</DestinationCity>' ||
                '<DestinationCountry>' || toString(x.13) || '</DestinationCountry>' ||
                '<ReceiverName>' || toString(x.14) || '</ReceiverName>' ||
                '<InvoiceNo>' || toString(x.15) || '</InvoiceNo>' ||
                '<LineItem/>' ||
                '<WeightValue>' || toString(x.16) || '</WeightValue>' ||
                '<Tariff>' || toString(x.17) || '</Tariff>' ||
                '<CODAmount>' || toString(x.18) || '</CODAmount>' ||
                '<BookingType>' || toString(x.19) || '</BookingType>' ||
                '<ContractNumber>' || toString(x.20) || '</ContractNumber>' ||
                '<Refrence/>' ||
                '<EventCode>' || toString(x.21) || '</EventCode>' ||
                '<EventDescription>' || toString(x.22) || '</EventDescription>' ||
                '<EventOfficeFacilityID>' || toString(x.23) || '</EventOfficeFacilityID>' ||
                '<EventOfficeName>' || toString(x.24) || '</EventOfficeName>' ||
                '<EventDate>' || toString(x.25) || '</EventDate>' ||
                '<EventTime>' || toString(x.26) || '</EventTime>' ||
                '<NonDelReason>' || toString(x.27) || '</NonDelReason>' ||
                '</ArticleDetails>',
            groupArray((
                cxcm.article_number,
                cxcm.article_type,
                formatDateTime(cxcm.booking_date, '%d%m%Y'),
                formatDateTime(cxcm.booking_time, '%H%i%s'),
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
                formatDateTime(cxcm.event_date, '%d%m%Y'),
                formatDateTime(cxcm.event_time, '%H%i%s'),
                ifNull(toString(cxcm.non_delivery_reason), '')
            ))
        )
    ) || 
    '</LatestEventDetails>' AS xml_output
FROM mis_db.amazon_target_table_dt_ib AS cxcm
WHERE cxcm.bulk_customer_id = 2000014074 
  AND cxcm.booking_date >= parseDateTime64BestEffort('2025-06-01 00:01:57.198516')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-21 00:01:57.198516')
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-06-01 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-21 00:01:57.198516')

SELECT DISTINCT ON (article_number) *
FROM mis_db.ext_mailbkg_mailbooking_dom
WHERE bulk_customer_id = 2000014074
  AND md_updated_date >= TIMESTAMP '2025-07-01 00:00:00'
  AND status_code = 'PC'
ORDER BY article_number, md_updated_date DESC;

  
  select * from mis_db.amazon_target_table_dt cxcm--1047141
  
WHERE cxcm.bulk_customer_id = 3000042033
  AND cxcm.booking_date >= parseDateTime64BestEffort('2025-07-01 00:01:57.198516')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-24 00:00:00.198516')
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-07-01 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-24 00:00:00.198516')
  
  select count(*) from mis_db.amazon_target_table_dt_ib cxcm---1150205
  
WHERE cxcm.bulk_customer_id = 3000042033
  AND cxcm.booking_date >= parseDateTime64BestEffort('2025-07-01 00:01:57.198516')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-24 00:00:00.198516')
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-07-01 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-24 00:00:00.198516')
  
  SELECT
    '<LatestEventDetails>' ||
    arrayStringConcat(
        arrayMap(
            x -> '<ArticleDetails>' ||
                '<ArticleNumber>' || toString(x.1) || '</ArticleNumber>' ||
                '<ArticleType>' || upper(toString(x.2)) || '</ArticleType>' || -- Converted to UPPER
                '<BookingDate>' || toString(x.3) || '</BookingDate>' ||
                '<BookingTime>' || toString(x.4) || '</BookingTime>' ||
                '<BookingOfficeFacilityID>' || upper(toString(x.5)) || '</BookingOfficeFacilityID>' || -- Converted to UPPER
                '<BookingOfficeName>' || upper(toString(x.6)) || '</BookingOfficeName>' || -- Converted to UPPER
                '<BookingPIN>' || toString(x.7) || '</BookingPIN>' ||
                '<SenderAddressCity>' || upper(toString(x.8)) || '</SenderAddressCity>' || -- Converted to UPPER
                '<DestinationOfficeFacilityID>' || upper(toString(x.9)) || '</DestinationOfficeFacilityID>' || -- Converted to UPPER
                '<DestinationOfficeName>' || upper(toString(x.10)) || '</DestinationOfficeName>' || -- Converted to UPPER
                '<DestinationPIN>' || toString(x.11) || '</DestinationPIN>' ||
                '<DestinationCity>' || upper(toString(x.12)) || '</DestinationCity>' || -- Converted to UPPER
                '<DestinationCountry>' || upper(toString(x.13)) || '</DestinationCountry>' || -- Converted to UPPER
                '<ReceiverName>' || upper(toString(x.14)) || '</ReceiverName>' || -- Converted to UPPER
                '<InvoiceNo>' || toString(x.15) || '</InvoiceNo>' ||
                '<LineItem/>' ||
                '<WeightValue>' || toString(x.16) || '</WeightValue>' ||
                '<Tariff>' || toString(x.17) || '</Tariff>' ||
                '<CODAmount>' || toString(x.18) || '</CODAmount>' ||
                '<BookingType>' || upper(toString(x.19)) || '</BookingType>' || -- Converted to UPPER
                '<ContractNumber>' || toString(x.20) || '</ContractNumber>' ||
                '<Refrence/>' ||
                '<EventCode>' || upper(toString(x.21)) || '</EventCode>' || -- Converted to UPPER
                '<EventDescription>' || upper(toString(x.22)) || '</EventDescription>' || -- Converted to UPPER
                '<EventOfficeFacilityID>' || upper(toString(x.23)) || '</EventOfficeFacilityID>' || -- Converted to UPPER
                '<EventOfficeName>' || upper(toString(x.24)) || '</EventOfficeName>' || -- Converted to UPPER
                '<EventDate>' || toString(x.25) || '</EventDate>' ||
                '<EventTime>' || toString(x.26) || '</EventTime>' ||
                '<NonDelReason>' || upper(toString(x.27)) || '</NonDelReason>' || -- Converted to UPPER
                '</ArticleDetails>',
            groupArray((
                cxcm.article_number,
                cxcm.article_type,
                formatDateTime(cxcm.booking_date, '%d%m%Y'),
                formatDateTime(cxcm.booking_time, '%H%i%s'),
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
                formatDateTime(cxcm.event_date, '%d%m%Y'),
                formatDateTime(cxcm.event_time, '%H%i%s'),
                ifNull(toString(cxcm.non_delivery_reason), '')
            ))
        )
    ) ||
    '</LatestEventDetails>' AS xml_output
FROM mis_db.amazon_target_table_dt_ib AS cxcm
WHERE cxcm.bulk_customer_id = 2000014074
  AND cxcm.booking_date >= parseDateTime64BestEffort('2025-06-01 00:01:57.198516')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-21 00:01:57.198516')
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-06-01 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-21 00:01:57.198516')
  --GROUP BY 1  -- Ensure single row output
--ORDER BY min(cxcm.booking_date); 
  
  SELECT
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
                '<ReceiverName>' || upper(toString(x.14)) || '</ReceiverName>' ||
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
            arraySort(
                x -> x.3,  -- Sort by booking date (formatted as %d%m%Y)
                groupArray((
                    cxcm.article_number,
                    cxcm.article_type,
                    formatDateTime(cxcm.booking_date, '%d%m%Y'),
                    formatDateTime(cxcm.booking_time, '%H%i%s'),
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
                    formatDateTime(cxcm.event_date, '%d%m%Y'),
                    formatDateTime(cxcm.event_time, '%H%i%s'),
                    ifNull(toString(cxcm.non_delivery_reason), '')
                ))
            )
        )
    ) ||
    '</LatestEventDetails>' AS xml_output
FROM mis_db.amazon_target_table_dt AS cxcm
WHERE cxcm.bulk_customer_id = 2000014074
  AND cxcm.booking_date >= parseDateTime64BestEffort('2025-06-01 00:01:57.198516')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-01 00:00:00.198516')
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-06-01 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-01 00:00:00.198516')
--GROUP BY 1  -- Ensure single row output
ORDER BY min(cxcm.booking_date);  -- Order by the earliest booking date in the result set

----
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
FROM mis_db.amazon_target_table_dt FINAL AS cxcm 
WHERE cxcm.bulk_customer_id = 2000014074
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-07-24 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-25 00:00:00.198516') and article_number ='MS003636900IN'

  
  SELECT count(*)
FROM (
    SELECT *
    FROM mis_db.amazon_target_table_dt_ib --FINAL
    WHERE bulk_customer_id = 2000014074
      --AND article_number = 'MS003636900IN'
      AND event_date >= parseDateTime64BestEffort('2025-06-01 00:00:00.198516')
      AND event_date < parseDateTime64BestEffort('2025-07-25 00:00:00.198516')
    ORDER BY event_date DESC, event_time DESC
   --LIMIT 1
)
-----
SELECT
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
                '<ReceiverName>' || upper(toString(x.14)) || '</ReceiverName>' ||
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
                formatDateTime(cxcm.booking_date, '%d%m%Y'),
                formatDateTime(cxcm.booking_time, '%H%i%s'),
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
                formatDateTime(cxcm.event_date, '%d%m%Y'),
                formatDateTime(cxcm.event_time, '%H%i%s'),
                ifNull(toString(cxcm.non_delivery_reason), '')
            ))
        )
    ) ||
    '</LatestEventDetails>' AS xml_output
FROM (
    SELECT *
    FROM (
        SELECT *,
            row_number() OVER (PARTITION BY article_number ORDER BY event_date DESC, event_time DESC) AS rn
        FROM mis_db.amazon_target_table_dt_ib FINAL
        WHERE bulk_customer_id = 2000014074
          AND event_date >= parseDateTime64BestEffort('2025-06-01 00:00:00.198516')
          AND event_date < parseDateTime64BestEffort('2025-07-25 00:00:00.198516')
    )
    WHERE rn = 1
) AS cxcm;

