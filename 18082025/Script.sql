SELECT 
    di.from_office_id,
    di.from_office_name,
    di.arrival_date,
    di.departure_date,
    di.total_bags,
    di.total_weight,
    --MAX(ah.arrival_id) AS trip_id,
    ah.arrival_id AS trip_id,
    di.space_occupied,
    ah.total_bags_received,
    ah.total_weight AS arrival_weight
FROM bagmgmt.departure_info di
JOIN bagmgmt.arrival_header ah 
    ON di.sch_id = ah.schedule_id
-- AND di.from_office_id = ah.office_id  -- Uncomment if required
WHERE di.sch_id = '1853'
AND di.from_office_id = 21860009  -- Proper array format
AND di.trip_start_date >= '2024-06-26'  -- Correct date format
AND di.trip_start_date <= '2024-06-26'
GROUP BY 
    di.from_office_id, di.from_office_name, di.arrival_date, 
    di.departure_date, di.total_bags, di.total_weight, 
    di.space_occupied, ah.total_bags_received, ah.total_weight,ah.arrival_id;

