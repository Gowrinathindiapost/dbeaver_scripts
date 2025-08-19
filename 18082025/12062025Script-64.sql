select * from carriermgmt.schedule s where schedule_id in ('202697','103359')
select * from carriermgmt.schedule where deleted_date is not null and schedule_status='ACTIVE'
select * from carriermgmt.schedule_stop_sequence where schedule_id='190854'
select * from carriermgmt.schedule_stop_sequence where schedule_id='93974'

SELECT *
FROM carriermgmt.schedule
WHERE schedule_status = 'ACTIVE'
  AND schedule_id NOT IN (
    SELECT schedule_id  -- Select the specific schedule_id column
    FROM carriermgmt.schedule_stop_sequence
  );

select * from carriermgmt.schedule_stop_sequence where schedule_id in (select * from carriermgmt.schedule where schedule_status='ACTIVE')

SELECT *
FROM carriermgmt.schedule_stop_sequence
WHERE schedule_id IN (
    SELECT schedule_id  -- Select the specific schedule_id column
    FROM carriermgmt.schedule
    WHERE schedule_status = 'ACTIVE'
);