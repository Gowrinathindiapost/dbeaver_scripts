WITH beClFiltered AS (
    SELECT 
        bcc.article_number, 
        be_cl.bag_number, 
        be_cl.to_office_id
    FROM bagmgmt.bag_close_content bcc
    JOIN bagmgmt.bag_event be_cl 
        ON bcc.bag_number = be_cl.bag_number
    WHERE be_cl.event_type IN ('CL', 'BL', 'TL', 'DP')
      AND be_cl.from_office_id = '124456789'
),
-- Subquery: beOpFiltered
beOpFiltered AS (
    SELECT 
        boc.article_number, 
        be_op.bag_number, 
        be_op.from_office_id
    FROM bagmgmt.bag_open_content boc
    JOIN bagmgmt.bag_event be_op 
        ON boc.bag_number = be_op.bag_number
    WHERE be_op.event_type IN ('OP', 'OR')
      AND be_op.to_office_id = '124456789'
)
SELECT DISTINCT ON (iv.article_bag_number)
    iv.article_bag_number,
    iv.art_type,
    iv.booking_office AS "ClosedByOffice",
    la.article_weight AS weight,
    iv.received_by,
    iv.approved_status,
    be_cl.bag_number AS "ClosedBagNumber",
    kom_cl.office_name AS "ClosedOfficeName",
    be_op.bag_number AS "OpenedBagNumber",
    kom_op.office_name AS "OpenedOfficeName",
    la.destn_office_pin AS "DestinationPin",
    la.booking_office_name AS "BookingOfficeName"
FROM bagmgmt.insured_verification iv
-- LATERAL JOIN to get the latest article_induction_stg data per article
LEFT JOIN LATERAL (
    SELECT
        article_weight,
        destn_office_pin,
        booking_office_name
    FROM bagmgmt.article_induction_stg a
    WHERE a.article_number = iv.article_bag_number
    ORDER BY art_stg_date DESC
    LIMIT 1
) la ON true
LEFT JOIN beClFiltered AS be_cl 
    ON iv.article_bag_number = be_cl.article_number
LEFT JOIN bagmgmt.kafka_office_master kom_cl 
    ON be_cl.to_office_id = kom_cl.office_id
LEFT JOIN beOpFiltered AS be_op 
    ON iv.article_bag_number = be_op.article_number
LEFT JOIN bagmgmt.kafka_office_master kom_op 
    ON be_op.from_office_id = kom_op.office_id
WHERE iv.office = '124456789'
  AND iv.set_date = :set_date
  AND iv.set_name = :set_number
  AND iv.art_type <> 'BG'
  AND iv.approved_status IN ('V', 'R')
ORDER BY iv.article_bag_number;

  SELECT 
       *
    FROM bagmgmt.bag_open_content boc
    
    
    SELECT 
       *
    FROM bagmgmt.bag_close_content bcc
    JOIN bagmgmt.bag_event be_cl 
        ON bcc.bag_number = be_cl.bag_number
    WHERE be_cl.event_type IN ('CL', 'BL', 'TL', 'DP')
      AND be_cl.from_office_id = '124456789'
      
      
      SELECT *
FROM bagmgmt.insured_verification iv
