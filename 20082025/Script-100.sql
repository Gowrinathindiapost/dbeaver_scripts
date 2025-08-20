
---query 1 GetInsuredArticleDetails
explain analyze 
SELECT DISTINCT ON (iv.article_bag_number)
    iv.article_bag_number,
    iv.art_type,
    iv.booking_office AS ClosedByOffice,
    iv.weight,
    iv.received_by,
    iv.approved_status,
    iv.set_date AS SetDate,
    iv.receive_approved_date AS ReceiveApprovedDate,
    iv.close_approved_date AS ClosedApprovedDate,
    iv.event_type,
    be_cl.bag_number AS ClosedBagNumber,
    kom_cl.office_name AS ClosedOfficeName,
    be_op.bag_number AS OpenedBagNumber,
    kom_op.office_name AS OpenedOfficeName,
    la.destn_office_pin AS DestinationPin,
    la.booking_office_name AS BookingOfficeName
FROM bagmgmt.insured_verification iv
LEFT JOIN bagmgmt.bag_open_content boc 
    ON boc.article_number = iv.article_bag_number
LEFT JOIN bagmgmt.bag_event be_op 
    ON be_op.bag_number = boc.bag_number 
    AND be_op.event_type IN ('OP', 'OR') 
    AND be_op.to_office_id = :office
LEFT JOIN bagmgmt.bag_close_content bcc 
    ON bcc.article_number = iv.article_bag_number 
    and bcc.set_date BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE
    --=iv.set_date  --check this later
LEFT JOIN bagmgmt.bag_event be_cl 
    ON be_cl.bag_number = bcc.bag_number 
    AND be_cl.from_office_id = :office 
    AND be_cl.event_type IN ('CL', 'BL', 'TL', 'DP')
LEFT JOIN bagmgmt.kafka_office_master kom_cl 
    ON kom_cl.office_id = be_cl.to_office_id
LEFT JOIN bagmgmt.kafka_office_master kom_op 
    ON kom_op.office_id = be_op.from_office_id
LEFT JOIN LATERAL (
    SELECT destn_office_pin, booking_office_name
    FROM bagmgmt.article_induction_stg
    WHERE article_number = iv.article_bag_number
      AND art_stg_date BETWEEN CURRENT_DATE - INTERVAL '5 days' AND CURRENT_DATE
    ORDER BY art_stg_date DESC
    LIMIT 1
) la ON TRUE
WHERE 
    iv.office = :office
    AND iv.set_name = 'SET1'
    AND iv.approved_status = :approveStatus
    AND iv.art_type != 'BG'
ORDER BY iv.article_bag_number, iv.set_date;

EXPLAIN SELECT * FROM bag_close_content WHERE set_name = 'SET1';

--query 2  GetExpextedArticlesforShiftClose
explain analyze 
SELECT DISTINCT
    ais.article_weight AS ArticleWeight,
    boc.article_number AS ArticleNumber,
    boc.article_type AS ArticleType,
    art_status AS ArtStatus,
    ais.booking_office_name AS BookingOfficeName,
    ais.destn_office_pin AS DestnOfficePin,
    boc.insured_flag AS InsuredFlag,
    false AS BulkDelStatus,
    kar.is_recall AS Recall,
    kar.is_return AS Return,
    ais.booking_reference_id AS BookingReferenceID
FROM bagmgmt.bag_open_content boc
LEFT JOIN bagmgmt.article_induction_stg ais
    ON boc.article_number = ais.article_number
    AND ais.art_stg_date BETWEEN NOW() - INTERVAL '5 days' AND NOW()
LEFT JOIN bagmgmt.kafka_article_recall_return kar
    ON boc.article_number = kar.article_number
WHERE
    boc.bag_number IN (
        SELECT bag_number
        FROM bagmgmt.bag_event
        WHERE
            to_office_id = 21260551
            AND set_date = '2025-07-15'
            AND set_number = 'SET1'
            AND event_type = 'OR'
    )
    AND boc.set_date BETWEEN NOW() - INTERVAL '7 days' AND NOW()
    AND art_status IN ('O', 'R')
    AND boc.article_type <> 'BG';

--SELECT DISTINCT
--    ais.article_weight AS ArticleWeight,
--    boc.article_number AS ArticleNumber,
--    boc.article_type AS ArticleType,
--    art_status AS ArtStatus,
--    ais.booking_office_name AS BookingOfficeName,
--    ais.destn_office_pin AS DestnOfficePin,
--    boc.insured_flag AS InsuredFlag,
--    false AS BulkDelStatus,
--    kar.is_recall AS Recall,
--    kar.is_return AS Return,
--    ais.booking_reference_id AS BookingReferenceID
--FROM bagmgmt.bag_open_content boc
--LEFT JOIN bagmgmt.article_induction_stg ais
--    ON boc.article_number = ais.article_number
--    AND ais.art_stg_date BETWEEN NOW() - INTERVAL '5 days' AND NOW()
--    and boc.set_date BETWEEN NOW() - INTERVAL '7 days' AND NOW()
--LEFT JOIN bagmgmt.kafka_article_recall_return kar
--    ON boc.article_number = kar.article_number
--WHERE
--    boc.bag_number IN (
--        SELECT bag_number
--        FROM bagmgmt.bag_event
--        WHERE
--            to_office_id = :toOfficeID
--            AND set_date = :setDate
--            AND set_number = :setNumber
--            AND event_type = 'OR'
--    )
--    AND art_status IN ('O', 'R')
--    AND boc.article_type <> 'BG';
---paritioning happened or not below query
SELECT inhrelid::regclass AS child_table
FROM pg_inherits
WHERE inhparent = 'bagmgmt.bag_open_content'::regclass;


----query 3 GetBagDetailsForManifest
explain analyze
SELECT
    bes.bag_number AS BagNumber,
    bes.transaction_date AS TranscationDate,
    om1.office_name AS FromOfficeName,
    om2.office_name AS ToOfficeName,
    bag_type AS BagType,
    delivery_type AS DeliveryType,
    from_office_id AS FromOfficeID,
    to_office_id AS ToOfficeID,
    bag_weight AS BagWeight,
    article_weight AS ArticleWeight,
    article_count AS ArticleCount,
    bag_count AS BagCount,
    event_type AS EventType,
    user_id AS UserID,
    bes.set_number AS SetNumber,
    schedule_id AS ScheduleID,
    bes.set_date AS SetDate,
    jsonb_agg(
        json_build_object(
            'article_number', bccs.article_number,
            'article_type', bccs.article_type,
            'art_status', bccs.art_status,
            'insured_flag', bccs.insured_flag
        )
    ) AS BagCloseArt
FROM bagmgmt.bag_event bes
JOIN bagmgmt.bag_open_content bccs
    ON bes.bag_number = bccs.bag_number
     and bccs.set_date BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE
JOIN bagmgmt.kafka_office_master om1
    ON bes.from_office_id = om1.office_id
JOIN bagmgmt.kafka_office_master om2
    ON bes.to_office_id = om2.office_id
WHERE
    bes.bag_number = :bagnumber
    AND bes.event_type IN ('OP', 'OR')
GROUP BY
    bes.bag_number,
    bag_type,
    delivery_type,
    from_office_id,
    to_office_id,
    bag_weight,
    article_weight,
    article_count,
    bag_count,
    event_type,
    user_id,
    bes.set_number,
    schedule_id,
    om1.office_name,
    om2.office_name,
    bes.transaction_date,
    bes.set_date
ORDER BY
    bes.transaction_date DESC
LIMIT 1;



select * from bag_close_content where article_number='EBK7012021199' and article_type='BG' and set_date BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE
