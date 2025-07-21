select * from bagmgmt.kafka_mailbooking_dom 
where article_number in ('CY000391506IN','CY000390892IN')


select * from bagmgmt.article_induction_stg 
where article_number in
('CY000391506IN','CY000390892IN')



select * from bagmgmt.article_induction_stg 
where article_number in
('EY250310001IN','EY250310002IN','EY250310003IN','EY100325001IN','EY100325002IN','EY100325003IN')

SELECT 
    di.from_office_id,
    di.from_office_name,
    di.arrival_date,
    di.departure_date,
    di.total_bags,
    di.total_weight,
    MAX(ah.arrival_id) AS trip_id,
    di.space_occupied,
    ah.total_bags_received,
    ah.total_weight AS arrival_weight
FROM bagmgmt.departure_info di
JOIN bagmgmt.arrival_header ah 
    ON di.sch_id = ah.schedule_id
-- AND di.from_office_id = ah.office_id  -- Uncomment if required
WHERE di.sch_id = $1
AND di.from_office_id = ANY($2)  -- Using an array for multiple touchpoints
AND di.trip_start_date >= $3
AND di.trip_start_date <= $4
GROUP BY 
    di.from_office_id, di.from_office_name, di.arrival_date, 
    di.departure_date, di.total_bags, di.total_weight, 
    di.space_occupied, ah.total_bags_received, ah.total_weight;
----a
SELECT 
    di.from_office_id,
    di.from_office_name,
    di.arrival_date,
    di.departure_date,
    di.total_bags,
    di.total_weight,
    MAX(ah.arrival_id) AS trip_id,
    di.space_occupied,
    ah.total_bags_received,
    ah.total_weight AS arrival_weight
FROM bagmgmt.departure_info di
JOIN bagmgmt.arrival_header ah 
    ON di.sch_id = ah.schedule_id
-- AND di.from_office_id = ah.office_id  -- Uncomment if required
WHERE di.sch_id = 1854
AND di.from_office_id = ANY(ARRAY[21860009])  -- Proper array format
AND di.trip_start_date >= '2024-06-26'  -- Correct date format
AND di.trip_start_date <= '2024-06-26'
GROUP BY 
    di.from_office_id, di.from_office_name, di.arrival_date, 
    di.departure_date, di.total_bags, di.total_weight, 
    di.space_occupied, ah.total_bags_received, ah.total_weight;

