SELECT DISTINCT ON (iv.article_bag_number)
    iv.article_bag_number,
    iv.art_type,
    iv.booking_office AS ClosedByOffice,
    la.article_weight AS weight,
    iv.received_by,
    iv.approved_status,
    be_cl.bag_number AS ClosedBagNumber,
    kom_cl.office_name AS ClosedOfficeName,
    be_op.bag_number AS OpenedBagNumber,
    kom_op.office_name AS OpenedOfficeName,
    la.destn_office_pin AS DestinationPin,
    la.booking_office_name AS BookingOfficeName
FROM bagmgmt.insured_verification iv
LEFT JOIN (
    -- beClFilteredSQL
) AS be_cl ON iv.article_bag_number = be_cl.article_number
LEFT JOIN bagmgmt.kafka_office_master kom_cl ON be_cl.to_office_id = kom_cl.office_id
LEFT JOIN (
    -- beOpFilteredSQL
) AS be_op ON iv.article_bag_number = be_op.article_number
LEFT JOIN bagmgmt.kafka_office_master kom_op ON be_op.from_office_id = kom_op.office_id
LEFT JOIN (
    -- latestArticleSQL
) AS la ON iv.article_bag_number = la.article_number
WHERE iv.office = :office_id
  AND iv.set_date = :set_date
  AND iv.set_name = :set_number
  AND iv.art_type <> 'BG'
  AND iv.approved_status IN ('V', 'R')
  
  select * from kafka_office_hierarchy_master ke where office_name like '%suru%'--mysuru nsh 21460007
