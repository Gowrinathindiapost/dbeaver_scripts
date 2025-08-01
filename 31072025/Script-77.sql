SELECT schedule_id ScheduleId, sequence_number SequenceNumber, stop_office_id StopOfficeId, 
stop_office_name StopOfficeName, next_stop_office_id NextStopOfficeId, next_stop_office_name NextStopOfficeName, 
distance_to_next_stop DistanceToNextStop, distance_unit DistanceUnit, transit_day TransitDay, transit_hour TransitHour, 
transit_minute TransitMinute, stay_duration_day StayDurationDay, stay_duration_hour StayDurationHour, 
stay_duration_minute StayDurationMinute, to_char(departure_time, 'HH24:MI') as DepartureTime, 
to_char(arrival_time, 'HH24:MI') as ArrivalTime, created_by CreatedBy, created_date CreatedDate, 
updated_by UpdatedBy, updated_date UpdatedDate, deleted_by DeletedBy, deleted_date DeletedDate 
FROM carriermgmt.schedule_stop_sequence_stg WHERE schedule_id = '379250' ORDER BY sequence_number LIMIT 1000
select * from schedule where schedule_id in (select schedule_id from carriermgmt.schedule_stop_sequence where sequence_number='13')
and schedule_status='ACTIVE'
select * from carriermgmt.schedule_stop_sequence where sequence_number='13'