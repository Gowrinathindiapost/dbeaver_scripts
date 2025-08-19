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
        WHERE bulk_customer_id = 1000002954
          AND article_number IN [AW778126201IN
AW778173144IN
AW778183787IN
AW778135415IN
AW805409584IN
AW778045525IN
AW778013505IN
AW778064277IN
AW778185258IN
AW777992814IN
AW805497028IN
EZ772006673IN
AW778046463IN
AW805476558IN
AW778157433IN
AW778111051IN
AW778082505IN
AW805291298IN
EZ771977875IN
AW777911299IN
AW778015126IN
AW778046826IN
AW778034814IN
AW778027393IN
EZ771993343IN
AW805328727IN
AW778050635IN
AW778038728IN
AW778033694IN
AW777943915IN
AW778035797IN
AW805460062IN
AW778049972IN
AW777996997IN
AW778035602IN
AW777938699IN
AW778048455IN
AW778033898IN
AW777971363IN
AW778013068IN
AW778035633IN
AW805416832IN
EZ771999788IN
AW778045366IN
AW777930908IN
AW777958340IN
AW778043025IN
AW805400347IN
AW778032932IN
AW778033408IN
AW778034880IN
AW778034173IN
AW778021078IN
AW805423524IN
AW778053495IN
AW777934017IN
AW778038285IN
AW777883933IN
AW778034332IN
AW778032420IN
EZ771999289IN
AW778024627IN
AW778040908IN
AW777945505IN
AW805309918IN
AW778025494IN
AW778025874IN
AW778027963IN
AW778049120IN
AW778044578IN
AW778001453IN
AW777973660IN
AW777945010IN
AW777933440IN
AW777981873IN
EZ772003677IN
AW778048787IN
AW777922929IN
AW778012005IN
AW778040488IN
AW778032901IN
AW778035148IN
AW778039842IN
AW805412291IN
AW778043484IN
AW805465918IN
AW777959756IN
AW777958530IN
EZ772001852IN
AW778031945IN
AW777990172IN
AW778010486IN
AW778056430IN
AW778037475IN
AW778045817IN
AW778025145IN
AW778034669IN
AW778026945IN
AW778053535IN
AW777978137IN
AW778025976IN
AW778052000IN
AW778037095IN
AW778026526IN
AW805426066IN
AW778034187IN
AW778007059IN
AW778025888IN
AW777902969IN
AW778054703IN
AW778034482IN
AW805381841IN
AW777949453IN
AW778024851IN
--AW777994165IN
--AW777955428IN

] -- Replace with your array of article numbers
    ) AS ranked
    WHERE rn = 1
) AS filtered_data