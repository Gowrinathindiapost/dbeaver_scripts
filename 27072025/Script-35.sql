
explain (analyse, buffers)
SELECT DISTINCT
  boc.article_number,
  boc.article_type,
  boc.art_status,
  COALESCE(
    CASE
      WHEN boc.article_type = 'BG'
      THEN bt.closed_from_office_name
      ELSE ais.booking_office_name
    END,
    ''
  ) AS booking_office_name,
  COALESCE(
    CASE
      WHEN boc.article_type = 'BG'
      THEN kom.pincode
      ELSE ais.destn_office_pin
    END,
    0
  ) AS destination_office_pin,
  boc.insured_flag,
  kar.is_recall,
  kar.is_return,
  boc.booking_reference_id,
  COALESCE(
    CASE
      WHEN boc.article_type = 'BG'
      THEN bt.bag_weight
      ELSE ais.article_weight
    END,
    0.0
  ) AS article_weight
FROM bagmgmt.bag_close_content AS boc
LEFT JOIN bagmgmt.kafka_article_recall_return AS kar
  ON boc.article_number = kar.article_number
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
    ON ais1.article_number = ais2.article_number AND ais1.art_stg_date = ais2.max_date
) AS ais
  ON boc.article_number = ais.article_number
LEFT JOIN bagmgmt.bag_transaction AS bt
  ON boc.article_number = bt.bag_number
LEFT JOIN bagmgmt.kafka_office_master AS kom
  ON bt.closed_to_office_id = kom.office_id
WHERE
  boc.bag_number = 'LBK3016580283';

select * from kafka_article_transaction where article_number= 'UI620480700IN''JD122908387IN' 'MP186472603IN' current_office_id='21105997' and  article_number='MP186472603IN''RK666745434IN' action_code in (3,4) and
