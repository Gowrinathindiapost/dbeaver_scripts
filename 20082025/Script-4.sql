select * from schedule 
UPDATE schedule 
SET schedule_valid_to = '2026-02-19 18:30:00.000';
SELECT * FROM schedule WHERE schedule_valid_to != '2026-02-19 18:30:00.000';
select * from kafka_office_hierarchy_master kohm where office_id=21630000

select distinct ( schedule_type,transport_mode,bag_type) from schedule 
select * from carrier_schedule_mapping where schedule_id='1856'
select * from vehicle where maintenance_mms = '21630000' vehicle_reg_number='KA01AB1234'
select * from schedule_stop_sequence where schedule_id=1856 order by sequence_number
