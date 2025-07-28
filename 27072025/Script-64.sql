select * from carriermgmt.schedule where schedule_id='115181 '--27660271--27680004
select * from carriermgmt.schedule_stop_sequence where schedule_id='115181'
select * from carriermgmt.kafka_office_hierarchy_master where office_id in ('27660271','27680004')

select * from carriermgmt.schedule where schedule_id='59263  '--27660271--27680004--27300001
select * from carriermgmt.schedule_stop_sequence where schedule_id='59263' 
select * from carriermgmt.schedule where schedule_create_office_id='27300001' and schedule_status='ACTIVE'