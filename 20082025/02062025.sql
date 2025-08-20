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
--        AND te.event_date BETWEEN parseDateTime64BestEffort('2025-07-31 00:00:00.000000') 
--                             AND parseDateTime64BestEffort('2025-08-09 23:59:59.000000')
--                              
      AND cxcm.article_number IN (
      'AW773261624IN',
'AW804221313IN',
'AW773077985IN',
'AW774510946IN',
'AW804365723IN',
'AW773163595IN',
'AW774623293IN',
'AW804216874IN',
'AW773988451IN',
'AW804306901IN',
'AW804246536IN',
'AW804314783IN',
'AW773593856IN',
'AW774641703IN',
'AW804313981IN',
'AW804210956IN',
'AW804241729IN',
'AW773476355IN',
'AW804311861IN',
'AW773599805IN',
'AW773439242IN',
'AW804227483IN',
'AW804329332IN',
'AW804330565IN',
'AW804393446IN',
'AW774324759IN',
'AW774567177IN',
'AW773980603IN',
'AW804327420IN',
'AW804224439IN',
'AW774402575IN',
'AW804385060IN',
'AW773855910IN',
'AW804330392IN',
'AW773795235IN',
'AW804391520IN',
'AW774136870IN',
'AW773654688IN',
'AW773276120IN',
'AW774233837IN',
'AW804240547IN',
'EZ771408812IN',
'AW804214119IN',
'AW773874406IN',
'AW772762615IN',
'AW774290882IN',
'AW773054352IN',
'AW804251085IN',
'AW804357700IN',
'AW773948700IN',
'AW804306892IN',
'AW774350011IN',
'AW804297776IN',
'AW774232451IN',
'EZ771193952IN',
'AW804226942IN',
'AW773132797IN',
'AW773132664IN',
'AW773227075IN',
'AW804439837IN',
'AW773717547IN',
'AW773962595IN',
'AW773741152IN',
'AW804213201IN',
'AW774616139IN',
'AW773292105IN',
'EZ771378996IN',
'AW804352353IN',
'AW774235872IN',
'AW774563688IN',
'AW804333734IN',
'EZ771151008IN',
'AW774573566IN',
'AW773212138IN',
'AW804229011IN',
'AW773877827IN',
'AW773793835IN',
'EZ771127879IN',
'AW773386377IN',
'AW774008884IN',
'AW773979548IN',
'AW774546968IN',
'AW774160750IN',
'AW804296634IN',
'AW773888436IN',
'AW774332931IN',
'AW773809844IN',
'AW774180312IN',
'AW804325242IN',
'AW804211483IN',
'AW774188007IN',
'AW774392484IN',
'AW773775672IN',
'AW804217415IN',
'AW804234100IN',
'AW774394882IN',
'AW804340806IN',
'AW773102645IN',
'AW773440104IN',
'AW774223809IN',
'AW804185885IN',
'AW804212815IN',
'AW773304554IN',
'AW804402620IN',
'AW804238251IN',
'AW804446577IN',
'AW804348345IN',
'AW774068976IN',
'AW804353265IN',
'AW773198303IN',
'AW774585119IN',
'AW774355359IN',
'AW773910386IN',
'AW774057562IN',
'AW773495916IN',
'AW804392278IN',
'AW773877521IN'
      )
      
) AS ranked
WHERE rn = 1