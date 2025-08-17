explain (analyze,buffers)
UPDATE carriermgmt.schedule
SET
    schedule_status = 'DELETE',
    deleted_by = '10178485', -- Replace with the actual user ID
    deleted_date = NOW() -- Or CURRENT_TIMESTAMP, depending on your SQL dialect
WHERE
    schedule_id IN (1) -- Replace with a comma-separated list of schedule IDs (e.g., 1, 2, 3)
and 
schedule_status IN ('ACTIVE', 'INACTIVE')
--    EXISTS (
--        SELECT 1
--        FROM carriermgmt.schedule
--        WHERE
--            schedule_status IN ('ACTIVE', 'INACTIVE')
--        AND
--            schedule_id IN (1) -- Replace with the same comma-separated list of schedule IDs
--    )

RETURNING
    schedule_id,
    schedule_name,
    schedule_type,
    bag_type,
    source_facility_id,
    source_facility_name,
    destination_facility_id,
    destination_facility_name,
    schedule_start_time,
    schedule_create_office_id,
    schedule_valid_from,
    schedule_valid_to,
    transport_mode,
    schedule_running_days,
    schedule_status,
    created_by,
    created_date,
    updated_by,
    updated_date,
    deleted_by,
    deleted_date;
--BNPL office id :
--BNPL under Mysore SSPOs Division 21530030 21250002
select * from carriermgmt.schedule  where source_facility_id='21250002' and destination_facility_id='21250002'
select * from carriermgmt.schedule_stop_sequence where stop_office_id='21250002' or next_stop_office_id='21250002'

select * from carriermgmt.kafka_office_hierarchy_master kohm where office_id='21360043''21250002'
--30002709
select * from carriermgmt.kafka_office_hierarchy_master where office_type_code ilike'%P%' and division_office_id='21530030'
--21260023 BPC
select * from carriermgmt.schedule where source_facility_id in ('21360043') or destination_facility_id in ('21360043')
select * from 
