SELECT *
FROM carriermgmt.schedule
WHERE
  source_facility_id = '29960001' AND
  destination_facility_id = '29860001' AND
  --'Speed Post' = ANY(bag_type) AND
  (
    (schedule_type = 'MMS Schedule' AND EXTRACT(EPOCH FROM (CAST(schedule_start_time AS time) - CAST(CURRENT_TIME AS time))) / 60 >= 180)
    OR
    (schedule_type IN ('TRAIN', 'MMS') AND EXTRACT(EPOCH FROM (CAST(schedule_start_time AS time) - CAST(CURRENT_TIME AS time))) / 60 >= 0)
  )
ORDER BY CAST(schedule_start_time AS time) ASC
LIMIT 1;
