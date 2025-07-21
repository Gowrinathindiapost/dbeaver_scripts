CBK3007400084
21360021

select * from bagmgmt.bag_open_content where bag_number='CBK3007400084'

select * from bagmgmt.bag_open_header_stg where static_bag_number='CBK3007400084'

select * from bagmgmt.bag_event where bag_number='CBK3007400084'

select * from bagmgmt.bag_event where to_office_id ='21360021' and date(transaction_date)= '2025-06-27' 
and event_type in ('RO','OP')


select * from bagmgmt.bag_event  a where a.to_office_id  =21360021
 and date(a.transaction_date)  = '2025-06-27' and event_type = 'OP'
 
 
 SELECT *
FROM bagmgmt.bag_event
WHERE to_office_id = '21360021'
  AND transaction_date > '2025-06-27 07:09:33.344';