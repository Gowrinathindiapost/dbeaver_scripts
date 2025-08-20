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
    WHERE cxcm.bulk_customer_id = 2000014074
        AND cxcm.booking_date < now()
        
        AND te.event_code <> 'ITEM_DELIVERY'
        AND te.event_code <> 'ITEM_BOOK'
--        AND te.event_date BETWEEN parseDateTime64BestEffort('2025-07-31 00:00:00.000000') 
--                             AND parseDateTime64BestEffort('2025-08-09 23:59:59.000000')
--                              
      AND cxcm.article_number IN (
  
      )
--      
) AS ranked
WHERE rn = 1