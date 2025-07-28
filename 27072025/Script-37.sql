SELECT count(s.schedule_id) as no_of_schedules, s.source_facility_name, s.source_facility_id, om.division_office_id, om.division_name FROM carriermgmt.schedule as s JOIN carriermgmt.kafka_office_hierarchy_master om ON s.source_facility_id = om.office_id WHERE s.source_facility_id IN ($1) GROUP BY s.source_facility_name, s.source_facility_id, om.division_office_id, om.division_name ORDER BY no_of_schedules ASC LIMIT 2147483647 OFFSET 0, string=args, []interface {}=[21530030])"}

SELECT 
    COUNT(s.schedule_id) AS no_of_schedules, 
    s.source_facility_name, 
    s.source_facility_id, 
    om.division_office_id, 
    om.division_name
FROM 
    carriermgmt.schedule AS s
JOIN 
    carriermgmt.kafka_office_hierarchy_master AS om 
    ON s.source_facility_id = om.office_id
WHERE 
    s.source_facility_id = $1
GROUP BY 
    s.source_facility_name, 
    s.source_facility_id, 
    om.division_office_id, 
    om.division_name
ORDER BY 
    no_of_schedules ASC
LIMIT 
    2147483647 OFFSET 0;
