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
    SELECT DISTINCT
        cxcm.article_number, cxcm.article_type,
        formatDateTime(cxcm.booking_date, '%d%m%Y') AS booking_date,
        formatDateTime(cxcm.booking_time, '%H%i%s') AS booking_time,
        cxcm.booking_office_facility_id,
        cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
        cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
        cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
        cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
        formatDateTime(te.event_date, '%d%m%Y') AS event_date,
        formatDateTime(te.event_date, '%H%i%s') AS event_time,
        te.event_code AS event_code,
        te.office_id AS event_office_facilty_id,
        te.office_name AS office_name,
        te.event_type AS event_description,
        te.delivery_status as non_delivery_reason
    FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
    INNER JOIN mis_db.new_customer_tracking_event_mv te
        ON cxcm.article_number = te.article_number
    LEFT JOIN mis_db.customer_log AS el
        ON cxcm.bulk_customer_id = el.customer_id
    WHERE cxcm.bulk_customer_id = 1000002954 --2000014100 --2000008005 --2000014074 --3000063972 --1000002954
      AND cxcm.booking_date >= toDateTime64('2025-08-01 00:00:00.000000', 6)
      AND cxcm.booking_date <= toDateTime64('2025-08-19 23:59:59.000000', 6)
      AND te.event_date >= toDateTime64('2025-08-01 00:00:00.000000', 6)
      AND te.event_date <= toDateTime64('2025-08-19 23:59:59.000000', 6)
      AND event_code= 'ITEM_DELIVERY'
) AS cxcm