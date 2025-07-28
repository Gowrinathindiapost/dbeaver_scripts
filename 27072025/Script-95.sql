-- Subquery: latest_article
explain analyse WITH latest_article AS (
    SELECT DISTINCT ON (article_number)
        article_number,
        destn_office_pin,
        booking_office_name,
        article_weight
    FROM article_induction_stg
    ORDER BY article_number, art_stg_date DESC
),
-- Subquery: beClFiltered
beClFiltered AS (
    SELECT 
        bcc.article_number, 
        be_cl.bag_number, 
        be_cl.to_office_id
    FROM bagmgmt.bag_close_content bcc
    JOIN bagmgmt.bag_event be_cl 
        ON bcc.bag_number = be_cl.bag_number
    WHERE be_cl.event_type IN ('CL', 'BL', 'TL', 'DP')
      AND be_cl.from_office_id = '21460007'
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
      AND be_op.to_office_id = '21460007'
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
LEFT JOIN beClFiltered AS be_cl 
    ON iv.article_bag_number = be_cl.article_number
LEFT JOIN bagmgmt.kafka_office_master kom_cl 
    ON be_cl.to_office_id = kom_cl.office_id
LEFT JOIN beOpFiltered AS be_op 
    ON iv.article_bag_number = be_op.article_number
LEFT JOIN bagmgmt.kafka_office_master kom_op 
    ON be_op.from_office_id = kom_op.office_id
LEFT JOIN latest_article AS la 
    ON iv.article_bag_number = la.article_number
WHERE iv.office = '21460007'
  AND iv.set_date = :set_date
  AND iv.set_name = :set_number
  AND iv.art_type <> 'BG'
  AND iv.approved_status IN ('V', 'R')
ORDER BY iv.article_bag_number;



------index required are below
CREATE INDEX idx_article_induction_stg_distinct ON article_induction_stg (article_number, art_stg_date DESC);
CREATE INDEX idx_be_event_type_from_office ON bag_event(from_office_id, event_type, bag_number);
CREATE INDEX idx_be_event_type_to_office ON bag_event(to_office_id, event_type, bag_number);


explain ANALYZE bagmgmt.insured_verification;
explain ANALYZE article_induction_stg;



-- Subquery: beClFiltered
explain analyse WITH beClFiltered AS (
    SELECT 
        bcc.article_number, 
        be_cl.bag_number, 
        be_cl.to_office_id
    FROM bagmgmt.bag_close_content bcc
    JOIN bagmgmt.bag_event be_cl 
        ON bcc.bag_number = be_cl.bag_number
    WHERE be_cl.event_type IN ('CL', 'BL', 'TL', 'DP')
      AND be_cl.from_office_id = '21460007'
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
      AND be_op.to_office_id = '21460007'
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
WHERE iv.office = '21460007'
  AND iv.set_date = :set_date
  AND iv.set_name = :set_number
  AND iv.art_type <> 'BG'
  AND iv.approved_status IN ('V', 'R')
ORDER BY iv.article_bag_number;


CREATE INDEX idx_article_lateral_covering 
ON bagmgmt.article_induction_stg (article_number, art_stg_date DESC)
INCLUDE (article_weight, destn_office_pin, booking_office_name);


