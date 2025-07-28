select * from carriermgmt.schedule s where schedule_id not in (select schedule_id from carriermgmt.schedule_stop_sequence)
and s.schedule_status ='ACTIVE'

SELECT s.*
FROM carriermgmt.schedule s
LEFT JOIN carriermgmt.schedule_stop_sequence sss 
  ON s.schedule_id = sss.schedule_id
WHERE sss.schedule_id IS NULL
  AND s.schedule_status = 'ACTIVE'
  and s.schedule_create_office_id ='11530001'

select * from carriermgmt.schedule s where schedule_id='377663'
select * from carriermgmt.schedule_stop_sequence where schedule_id='377663'

select * from carriermgmt.schedule s where schedule_id='377729'
select * from carriermgmt.schedule_stop_sequence where schedule_id='377729'

select * from carriermgmt.schedule s where schedule_id in ('175403','175414')
select * from carriermgmt.schedule_stop_sequence where schedule_id in ('175403','175414')

SELECT s.schedule_create_office_id, COUNT(*) AS inactive_schedule_count
FROM carriermgmt.schedule s
LEFT JOIN carriermgmt.schedule_stop_sequence sss 
  ON s.schedule_id = sss.schedule_id
WHERE sss.schedule_id IS NULL
  AND s.schedule_status = 'INACTIVE'  
GROUP BY s.schedule_create_office_id;

Join("carriermgmt.kafka_office_master AS kom ON s.source_facility_id = kom.office_id").
		Join("carriermgmt.kafka_office_hierarchy_master AS kohm ON kom.office_id = kohm.office_id").


select * from carriermgmt.schedule where schedule_id ='1915' in ('21660367')
select * from carriermgmt.kafka_office_hierarchy_master where office_id='21660379'--21530007
select * from carriermgmt.schedule where schedule_create_office_id ='21530007'
select * from carriermgmt.schedule where schedule_id in ('214412')
select * from carriermgmt.schedule_stop_sequence where schedule_id in ('214412')
select * from carriermgmt.schedule where schedule_id in ('388094')
select * from carriermgmt.schedule_stop_sequence where schedule_id in ('388094','1532','1530')


SELECT 
    s.schedule_create_office_id, 
    COUNT(*) AS schedules_without_stops
FROM carriermgmt.schedule s
LEFT JOIN carriermgmt.schedule_stop_sequence sss 
    ON s.schedule_id = sss.schedule_id
LEFT JOIN carriermgmt.kafka_office_master kom 
    ON s.schedule_create_office_id = kom.office_id
LEFT JOIN carriermgmt.kafka_office_hierarchy_master kohm 
    ON kom.office_id = kohm.office_id
WHERE sss.schedule_id IS NULL
  AND s.schedule_status in ('ACTIVE','INACTIVE') 
GROUP BY s.schedule_create_office_id;


SELECT 
    S.*,kohm.circle_code
FROM carriermgmt.schedule s
LEFT JOIN carriermgmt.schedule_stop_sequence sss 
    ON s.schedule_id = sss.schedule_id
LEFT JOIN carriermgmt.kafka_office_master kom 
    ON s.schedule_create_office_id = kom.office_id
LEFT JOIN carriermgmt.kafka_office_hierarchy_master kohm 
    ON kom.office_id = kohm.office_id
WHERE sss.schedule_id IS NULL
  AND s.schedule_status in ('ACTIVE','INACTIVE')  and s.schedule_create_office_id ='11300001'
GROUP BY s.schedule_create_office_id;


select * from carriermgmt.schedule where schedule_create_office_id='24530032'

