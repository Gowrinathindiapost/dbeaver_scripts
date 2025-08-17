LP805241291IN
select * from bagmgmt.article_induction_stg where article_number='LP805241291IN'
select * from bag_close_content where article_number='LP805241291IN'
select * from bag_open_content where article_number='LP805241291IN'
select * from bagmgmt.kafka_mailbooking_intl where article_number='LP805241291IN'

select * from 
15260002

SELECT
    article_number,
    booking_office_id,
    booking_office_name,
    article_type,
    article_weight,
    insured_flag,
    destn_office_pin,
    booking_reference_id,
    false AS recall,
    false AS return
FROM article_induction_stg
WHERE booking_office_id = '15260002'
  AND toDate(booking_date) = '2025-07-26 11:24:26.703'
  AND article_status = 'BKG'
  AND article_type IN ({articleTypes:Array(String)})
  
  ---27072025
  select * from bagmgmt.article_induction_stg where article_number='LP805255667IN' 'LP805255772IN'
select * from bag_close_content where article_number='LP805255772IN'
select * from bag_open_content where article_number='LP805255772IN'
select * from bagmgmt.kafka_mailbooking_intl where article_number='LP805255667IN' 'LP805255772IN'
select * from bagmgmt.kafka_INDUCTION_intERNATIONAl where article_number='LP805255667IN' 'LP805255772IN'
select * from bagmgmt.kafka_INDUCTION_DOMESTIC where article_number='LP805255667IN'  
  