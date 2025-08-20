SELECT DISTINCT 
    ais.article_weight AS ArticleWeight,
    boc.article_number AS ArticleNumber,
    boc.article_type AS ArticleType,
    boc.art_status AS ArtStatus,
    ais.booking_office_name AS BookingOfficeName,
    ais.destn_office_pin AS DestnOfficePin,
    boc.insured_flag AS InsuredFlag,
    FALSE AS BulkDelStatus,
    kar.is_recall AS Recall,
    kar.is_return AS Return,
    ais.booking_reference_id AS BookingReferenceID
FROM 
    bagmgmt.bag_open_content boc
LEFT JOIN 
    bagmgmt.article_induction_stg ais ON boc.article_number = ais.article_number
LEFT JOIN 
    bagmgmt.kafka_article_recall_return kar ON boc.article_number = kar.article_number
WHERE 
    boc.bag_number IN (
        SELECT bag_number 
        FROM bagmgmt.bag_event 
        WHERE 
            to_office_id = '21460007' 
            AND set_date = '2025-05-27' 
            AND set_number = 'SET1'
            and event_type = 'OR'
    )
    AND boc.art_status IN ('O', 'R','OP')
    AND boc.article_type <> 'BG';
21630041
select * from bagmgmt.kafka_office_master kom  where kom.office_id ='21661296'
select * from bagmgmt.kafka_office_master where office_name ilike 'orting'
