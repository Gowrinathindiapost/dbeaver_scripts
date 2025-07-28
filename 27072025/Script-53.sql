EBK3849384934--EBK3849384934--RY000028905IN
select * from bagmgmt.bag_open_content where article_number='RY000028879IN'
select * from bagmgmt.article_induction_stg where article_number='RY000028879IN'
select * from bagmgmt.bag_event where bag_number='EBX0000000082'
WITH max_article_date AS (
    SELECT article_number, MAX(art_stg_date) AS max_art_stg_date
    FROM bagmgmt.article_induction_stg
    GROUP BY article_number
)
SELECT DISTINCT
    boc.bag_number AS BagNumber,
    boc.article_number AS ArticleNumber,
    boc.article_type AS ArticleType,
    ais.destn_office_pin AS DestinationPin,
    ais.booking_office_name AS BookingOffice,
    boc.insured_flag AS InsuredFlag,
    be1.user_id AS UserID,
    omt.office_name AS OfficeName,
    boc.booking_reference_id AS BookingReferenceID
FROM bagmgmt.bag_open_content boc
INNER JOIN bagmgmt.bag_event be1
    ON boc.bag_number = be1.bag_number
    AND be1.event_type IN ('OP', 'OR')
    AND be1.set_number = 'SET1'
    AND be1.set_date = '2025-06-03'
    AND be1.to_office_id = '29460002'
LEFT JOIN max_article_date mad
    ON boc.article_number = mad.article_number
LEFT JOIN bagmgmt.article_induction_stg ais
    ON boc.article_number = ais.article_number
    AND ais.art_stg_date = mad.max_art_stg_date
LEFT JOIN bagmgmt.kafka_office_master omt
    ON be1.from_office_id = omt.office_id
WHERE boc.article_type <> 'BG'
  AND boc.art_status <> 'S'
ORDER BY
    boc.bag_number,
    boc.article_number,
    boc.article_type,
    ais.destn_office_pin,
    ais.booking_office_name,
    boc.insured_flag,
    be1.user_id,
    omt.office_name,
    boc.booking_reference_id;

-----
WITH max_article_date AS (SELECT article_number, MAX(art_stg_date) AS max_art_stg_date
FROM bagmgmt.article_induction_stg
GROUP BY article_number
)
SELECT DISTINCT boc.bag_number AS BagNumber, boc.article_number AS ArticleNumber, 
boc.article_type AS ArticleType, COALESCE(ais.destn_office_pin, '999999') AS DestinationPin, 
COALESCE(ais.booking_office_name, 'UNKNOWN') AS BookingOffice, 
boc.insured_flag AS InsuredFlag, be1.user_id AS UserID, 
COALESCE(omt.office_name, '') AS OfficeName,
boc.booking_reference_id AS BookingReferenceID 
FROM bagmgmt.bag_open_content boc JOIN 
bagmgmt.bag_event be1 ON boc.bag_number = be1.bag_number 
AND be1.event_type IN ('OP', 'OR') AND be1.set_number = $1 AND be1.set_date = $2 
AND be1.to_office_id = $3 LEFT JOIN bagmgmt.article_induction_stg ais 
ON boc.article_number = ais.article_number
LEFT JOIN max_article_date mad 
ON boc.article_number = mad.article_number 
AND ais.art_stg_date = mad.max_art_stg_date
LEFT JOIN bagmgmt.kafka_office_master omt ON be1.from_office_id = omt.office_id 
WHERE boc.article_type <> $4 AND boc.art_status <> $5 
ORDER BY boc.bag_number, boc.article_number, boc.article_type, DestinationPin, BookingOffice, 
boc.insured_flag, be1.user_id, OfficeName, boc.booking_reference_id