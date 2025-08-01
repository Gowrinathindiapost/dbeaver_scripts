select * from ips.ips_event ie 

select * from ips.office_master om 

select * from ips.ooe_mapping om where om.ooe_code ='AFKBLS'

select * from ips.article --RD220302500IN--if it is there in this table it is booked


select * from ips.ips_event ie where ie.article_number ='RD220302500IN'--SKBTSA


select * from ips.ooe_master om where  om.ooe_fcd ='INCCUB'--INBOMA

select * from ips.of


ooe_master
ips_event_master

select * from ips_event_master where event_cd=30
select distinct(DATE(a.event_time)) as Date,to_char(a.event_time, 'HH24:MI:SS') AS Time,a.article_number,a.event_cd ,a.office_cd,iem.event_name,om.ooe_name,om.pincode   
from ips.ips_event a --where a.article_number ='RD220302500IN' 
JOIN ips.ips_event_master iem on iem.event_cd= a.event_cd 
JOIN ips.ooe_master om on om.ooe_fcd= a.office_cd 
where a.article_number in('CA072771975IN') ('RD220302500IN','CT530202963IN','CE027445290SA','CE032988665SA')

select * from ips.article a where a.article_number ='RD220302500IN'

--date--event date
--time--event time
--country--
--location
--event_type
--mail category,
--next office
--remarks

