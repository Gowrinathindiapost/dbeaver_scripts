CBK3014298864
select * from bagmgmt.bag_event where bag_number='cbk3014298864' order by bag
RO---received for open
or--
RF--
DI--
RF--
select * from bagmgmt.bag_open_content where article_number  ='JO214298182IN'
JO214298182IN --rts article integrity constraint

select * from bagmgmt.kafka_article_transaction kat where article_number  ='JO214298182IN'
select * from bagmgmt.article_induction_stg where article_number  ='JO214298182IN'
select * from bagmgmt.bag_close_content where article_number  ='JO214298182IN'
EBK7014019820
select * from bagmgmt.bag_event where bag_number='EBK7014019820'
--
select distinct(art_status) from bagmgmt.bag_open_content 
select bag_number,article_number,article_type,art_status from bagmgmt.bag_open_content where art_status='C'
select * from bagmgmt.article_induction_stg
select * from bagmgmt.bag_close_content 
select distinct(art_status) from bagmgmt.bag_close_content
C--
S--short
E--excess
R--received at RMS
O--received at post office
D--disposed article
EBK7019300316
CDD6298990794
EBK7019304668
CDD6298990794
select * from bagmgmt.bag_event where bag_number in ('EBK7019300316','CDD6298990794','EBK7019304668','CDD6298990794')
EBK5012730029
EBT0020026933
EBT0020027068
EBT0020028306
EDA8200426186
EDG
select * from bagmgmt.bag_event where bag_number in ('EBK5012730029')
select * from bagmgmt.bag_open_content_stg where article_number in ('RK819252589IN','TA657293754IN','RK420577211IN','AV276951742IN','AV276951743IN','AV276951744IN','AV276951745IN','AV276951746IN','AV276951747IN','AV276951748IN') 
select * from bagmgmt.article_induction_stg where article_number in ('RK819252589IN','TA657293754IN','RK420577211IN','AV276951742IN','AV276951743IN','AV276951744IN','AV276951745IN','AV276951746IN','AV276951747IN','AV276951748IN')

select * from bagmgmt.bag_close_content where bag_number='LBK6016947870'
select * from bagmgmt.bag_event where bag_number='LBK6016947870' order by bag_event_id desc
select true as existornot from bag_event where bag_number='LBK6016947870'
and event_type='DI' and from_office_id='21106004'
Select("true as ExisitOrNot").
		From("bag_event").
		Where("(bag_number=? AND event_type = ? AND from_office_id=?)", bagnumber, eventType, fromOfficeID).
		Limit(1)
-----
SELECT
  DISTINCT boc.article_number AS ArticleNumber,
  ais.booking_office_id AS BookingOfficeID,
  ais.booking_office_name AS BookingOfficeName,
  boc.article_type AS ArticleType,
  ais.booking_office_pin AS BookingOfficePIN,
  ais.destn_office_pin AS DestinationOfficePIN,
  ais.booking_date AS BookingDatetime,
  ais.article_weight AS ArticleWeight,
  boc.insured_flag AS InsuredFlag,
  'BOA' AS InductionChannel,
  boc.booking_reference_id AS BookingID,
  NULL :: BIGINT AS BulkCustomerID
FROM bagmgmt.bag_open_content AS boc
LEFT JOIN (
  SELECT
    ais1.*
  FROM bagmgmt.article_induction_stg AS ais1
  INNER JOIN (
    SELECT
      article_number,
      MAX(art_stg_date) AS max_date
    FROM bagmgmt.article_induction_stg
    GROUP BY
      article_number
  ) AS ais2
  ON ais1.article_number = ais2.article_number
  AND ais1.art_stg_date = ais2.max_date
) AS ais
ON boc.article_number = ais.article_number
WHERE
  boc.bag_number IN (
    SELECT
      bag_number
    FROM bagmgmt.bag_event
    WHERE
      bag_type = 'BO' AND event_type = 'OP' AND to_office_id = ? AND set_number = 'SET1'
  ) AND boc.art_status = 'O' AND boc.article_number NOT IN (
    SELECT
      article_number
    FROM bagmgmt.article_induction_stg
    WHERE
      article_status = 'BKG' OR (
        article_status = 'CLS' AND induction_channel <> 'CSI'
      )
  );

--------------------------
select * from bagmgmt.bag_close_content boc where  article_number in 

('CK510912062IN',
'CK510912076IN',
'CK510912059IN',
'CK510912080IN',
'CK510912093IN',
'CK510912102IN',
'CK510912116IN',
'CK510912120IN')
SELECT
  be.bag_number,
  from_office.office_name AS from_office_name,
  to_office.office_name AS to_office_name,
  be.bag_weight,
  be.bag_type,
  be.insured_flag
FROM bagmgmt.bag_event AS be
JOIN bagmgmt.kafka_office_master AS from_office
  ON be.from_office_id = from_office.office_id
JOIN bagmgmt.kafka_office_master AS to_office
  ON be.to_office_id = to_office.office_id
WHERE
  be.bag_number in ('CBX0000000235') AND be.event_type IN ('CL', 'DP');
select * from bagmgmt.bag_event where bag_number='LBK6016947889'
select * from bagmgmt.bag_event where bag_number='LBK6017549613'
select * from bagmgmt.bag_event where  bag_number='LBK6020451866'
select * from bagmgmt.bag_event where  bag_number='LBK6016947897'
select * from bagmgmt.bag_event where  bag_number='LBK3016581879'--*
select * from bagmgmt.bag_event where  bag_number='LBK6016947890'
select * from bagmgmt.bag_event where 
-----

SELECT
  be1.*
FROM bagmgmt.bag_event AS be1
INNER JOIN (
  SELECT
    bag_number,
    transaction_date,
    COUNT(*)
  FROM bagmgmt.bag_event
  WHERE
    event_type = 'OP' AND bag_type = 'BO'
  GROUP BY
    bag_number,
    transaction_date
  HAVING
    COUNT(*) > 1
) AS be2
  ON be1.bag_number = be2.bag_number AND be1.transaction_date = be2.transaction_date
WHERE
  be1.event_type = 'OP' AND be1.bag_type = 'BO';