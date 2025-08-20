select * from bagmgmt.insured_verification where   office='21661272' article_bag_number='CK471810682IN' 
 and approved_status='R' and art_type <> 'BG'

select * from bagmgmt.insured_verification where  article_bag_number ='EK388164044IN'
select * from bagmgmt.insured_verification where  article_bag_number ='CK469777617IN'
select * from bagmgmt.insured_verification where  article_bag_number ='CK469777603IN'
select * from bagmgmt.insured_verification where  article_bag_number in ('EK388164044IN','CK469777617IN','CK469777603IN')
select * from bagmgmt.article_induction_stg where article_number in ('EK388164044IN','CK469777617IN','CK469777603IN')
select * from bagmgmt.bag_open_content where article_number in ('EK388164044IN','CK469777617IN','CK469777603IN')
select * from bagmgmt.kafka_office_hierarchy_master where office_id='21661272'