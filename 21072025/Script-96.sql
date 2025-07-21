

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
WHERE cxcm.bulk_customer_id = 1000002954
  AND cxcm.booking_date >= parseDateTime64BestEffort('2025-07-20 00:01:57.198516')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-21 00:01:57.198516')
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-07-20 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-21 00:01:57.198516')

  
  select * from mis_db.`amazon_target_table_dt` cxcm
  
WHERE cxcm.bulk_customer_id = 1000002954
  AND cxcm.booking_date >= parseDateTime64BestEffort('2025-07-20 00:01:57.198516')
  AND cxcm.booking_date < parseDateTime64BestEffort('2025-07-21 00:01:57.198516')
  AND cxcm.event_date >= parseDateTime64BestEffort('2025-07-20 00:01:57.198516')
  AND cxcm.event_date < parseDateTime64BestEffort('2025-07-21 00:01:57.198516')
