SELECT
    kmd.article_number AS "ArticleNumber",
    kmd.mail_type_code AS "ArticleType",
    to_char(kmd.md_created_date, 'YYYY-MM-DD') AS "BookingDate",
    to_char(kmd.md_created_date, 'HH24:MI:SS') AS "BookingTime",
    kmd.md_office_id_bkg AS "BookingOfficeFacilityID",
    kom.office_name AS "BookingOfficeName",
    kmd.origin_pincode AS "BookingPIN",
    kba1.city AS "SenderAddressCity",
    kmd.destination_office_id AS "DestinationOfficeFacilityID",
    kmd.destination_office_name AS "DestinationOfficeName",
    kmd.destination_pincode AS "DestinationPIN",
    kba2.city AS "DestinationCity",
    'INDIA' AS "DestinationCountry",
    kba2.addressee_name AS "ReceiverName",
    ''::text AS "InvoiceNo",
    ''::text AS "LineItem",
    kmd.charged_weight AS "WeightValue",
    kcd.total_amount AS "Tariff",
    0.0 AS "CODAmount",
    ''::text AS "BookingType",
    ''::text AS "ContractNumber",
    ''::text AS "Refrence"
FROM
    trackandtrace.kafka_mailbooking_dom kmd
JOIN
    trackandtrace.kafka_booking_address kba1 ON kmd.sender_address_reference = kba1.address_id
JOIN
    trackandtrace.kafka_booking_address kba2 ON kmd.receiver_address_reference = kba2.address_id
JOIN
    trackandtrace.kafka_charges_detail kcd ON kmd.charges_detail_id = kcd.charges_detail_id
JOIN
    trackandtrace.kafka_office_master kom ON kmd.md_office_id_bkg = kom.office_id
LEFT JOIN
    trackandtrace.kafka_article_event kat ON kmd.article_number = kat.article_number AND kat.event_code = 'ID'
WHERE
    kmd.article_number = 'EY014661896IN'
LIMIT 1;



---------------------
SELECT
    to_char(be.transaction_date, 'YYYY-MM-DD') AS "EventDate",
    to_char(be.transaction_date, 'HH24:MI:SS') AS "EventTime",
    CASE
        WHEN be.event_type IN ('CL','DI') THEN kom.office_name
        WHEN be.event_type IN ('RO','OP','OR') THEN kom2.office_name
        ELSE NULL
    END AS "EventOfficeName",
    CASE
        WHEN be.event_type = 'CL' THEN 'BAG_CLOSE'
        WHEN be.event_type IN ('OP','OR') THEN 'BAG_OPEN'
        WHEN be.event_type = 'DI' THEN 'BAG_DISPATCH'
        WHEN be.event_type = 'RO' THEN 'Bag Received'
    END AS "EventCode",
    CASE
        WHEN be.event_type = 'CL' THEN 'Bag Close'
        WHEN be.event_type IN ('OP','OR') THEN 'Bag Open'
        WHEN be.event_type = 'DI' THEN 'Bag Dispatch'
        WHEN be.event_type = 'RO' THEN 'Bag Received'
    END AS "EventDescription"
FROM
    trackandtrace.kafka_bag_event be
JOIN
    trackandtrace.kafka_office_master kom ON be.from_office_id = kom.office_id
JOIN
    trackandtrace.kafka_office_master kom2 ON be.to_office_id = kom2.office_id
JOIN (
    SELECT DISTINCT bag_number
    FROM trackandtrace.kafka_bag_close_content
    WHERE article_number = 'CY000015281IN'
    UNION
    SELECT DISTINCT bag_number
    FROM trackandtrace.kafka_bag_open_content
    WHERE article_number = 'CY000015281IN'
) AS BagNumbers ON be.bag_number = BagNumbers.bag_number
ORDER BY
    be.transaction_date DESC
LIMIT 1;
select * from trackandtrace.kafka_bag_event where
select * from trackandtrace.kafka_article_event where article_number= 'EY014661896IN'
-----------------------
SELECT 
    to_char(kae.event_date, 'YYYY-MM-DD') AS EventDate,
    to_char(kae.event_date, 'HH24:MI:SS') AS EventTime,
    kom.office_name AS EventOfficeName,
    kae.current_office_id AS EventOfficeFaciltyID,
    kae.remarks AS EventDescription
FROM trackandtrace.kafka_article_event kae
JOIN trackandtrace.kafka_office_master kom ON kae.current_office_id = kom.office_id
WHERE kae.article_number = 'CY000015281IN'
ORDER BY kae.event_date DESC
LIMIT 1;
select * from trackandtrace.kafka_article_transaction where article_number='EY014538449IN'