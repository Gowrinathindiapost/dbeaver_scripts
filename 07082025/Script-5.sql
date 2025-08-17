select * from bagmgmt.kafka_article where article_number='EY250310001IN'
select * from bagmgmt.kafka_article_transaction where article_number='EY250310001IN'
select * from bagmgmt.kafka_mailbooking_dom 
where article_number in ('CY000390981IN')
('EY250310001IN','EY250310002IN','EY250310003IN','EY100325001IN','EY100325002IN','EY100325003IN')

select * from bagmgmt.article_induction_stg 
where article_number in
('EY250310001IN','EY250310002IN','EY250310003IN','EY100325001IN','EY100325002IN','EY100325003IN')



