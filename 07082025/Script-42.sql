SELECT
    be.user_id,
    TRIM(CONCAT_WS(' ', kem.employee_first_name, kem.employee_middle_name, kem.employee_last_name)) AS full_name,
    SUM(CASE
        WHEN be.event_type IN ('OP', 'OR')
        AND be.to_office_id = 21070745
        AND be.set_number = 'SET1'-- **Quoted date string**
        AND be.set_date = '2024-07-05'        -- **Quoted string value**
        AND be.user_id != 0
        AND be.user_id IS NOT NULL THEN be.article_count
        ELSE 0
    END) AS received_articles,
    SUM(CASE
        WHEN be.event_type IN ('CL', 'BL', 'DP')
        AND be.from_office_id = 21070745
        AND be.set_number = 'SET1'      -- **Quoted string value**
        AND be.set_date = '2024-07-05' -- **Quoted date string**
        AND be.user_id != 0
        AND be.user_id IS NOT NULL THEN be.article_count
        ELSE 0
    END) AS dispatched_articles
FROM
    bagmgmt.bag_event be
LEFT JOIN
    bagmgmt.kafka_employee_master kem ON kem.employee_id = be.user_id
WHERE
    be.set_number = 'SET1'    -- **Quoted string value**
    AND be.set_date = '2024-07-05' -- **Quoted date string**
    AND be.user_id != 0
    AND be.user_id IS NOT NULL
GROUP BY
    be.user_id, full_name
HAVING
    COUNT(CASE
        WHEN be.event_type IN ('OP', 'OR')
        AND be.to_office_id = 21070745
        AND be.set_number =  'SET1'-- **Quoted date string**
        AND be.set_date = '2024-07-05'        -- **Quoted string value**
        AND be.user_id != 0
        AND be.user_id IS NOT NULL THEN 1
    END) > 0
    OR
    COUNT(CASE
        WHEN be.event_type IN ('CL', 'BL', 'DP')
        AND be.from_office_id = 21070745
        AND be.set_number = 'SET1'      -- **Quoted string value**
        AND be.set_date = '2024-07-05' -- **Quoted date string**
        AND be.user_id != 0
        AND be.user_id IS NOT NULL THEN 1
    END) > 0;
select * from bagmgmt.kafka_employee_master kem where employee_id= '10062207' '10045110'
select * from bagmgmt.bag_event where bag_number='CPB0000000822'

SELECT
    bes.bag_number,
    om1.office_name AS from_office_name,
    om2.office_name AS to_office_name,
    be.customer_name,
    be.customer_id,
    bes.bag_type,         -- Assuming from 'bes'
    bes.delivery_type,    -- Assuming from 'bes'
    bes.transaction_date, -- Assuming from 'bes'
    bes.set_date,         -- Assuming from 'bes'
    bes.from_office_id,   -- Assuming from 'bes'
    bes.to_office_id,     -- Assuming from 'bes'
    bes.bag_weight,       -- Assuming from 'bes'
    bes.article_weight,   -- Assuming from 'bes'
    bes.article_count,    -- Assuming from 'bes'
    bes.bag_count,        -- Assuming from 'bes'
    bes.event_type,       -- Assuming from 'bes'
    bes.user_id,          -- Assuming from 'bes'
    TRIM(CONCAT_WS(' ', kem.employee_first_name, kem.employee_middle_name, kem.employee_last_name)) AS full_name,
    bes.set_number,       -- Assuming from 'bes'
    bes.schedule_id,      -- Assuming from 'bes'
    jsonb_agg(json_build_object(
        'article_number', bccs.article_number,
        'article_type', bccs.article_type,
        'art_status', bccs.art_status,
        'insured_flag', bccs.insured_flag
    )) AS bag_info
FROM
    bagmgmt.bag_event bes
    join 
	bagmgmt.kafka_employee_master kem ON bes.user_id = kem.employee_id

JOIN
    bagmgmt.bag_close_content bccs ON bes.bag_number = bccs.bag_number
JOIN
    bagmgmt.kafka_office_master om1 ON bes.from_office_id = om1.office_id
JOIN
    bagmgmt.kafka_office_master om2 ON bes.to_office_id = om2.office_id
JOIN
    bagmgmt.bag_event_bulk be ON bes.bag_number = be.bag_number

WHERE
    bes.bag_number = 'CPB0000000822'
GROUP BY
    bes.bag_number,
    bes.bag_type,
    bes.transaction_date,
    bes.set_date,
    bes.delivery_type,
    bes.from_office_id,
    bes.to_office_id,
    bes.bag_weight,
    bes.article_weight,
    bes.article_count,
    bes.bag_count,
    bes.event_type,
    bes.user_id,
    bes.set_number,
    bes.schedule_id,
    om1.office_name,
    om2.office_name,
    be.customer_name,
    be.customer_id,
    full_name
ORDER BY
    bes.transaction_date
LIMIT 1;
select * from bagmgmt.kafka_employee_master kem where employee_id='80001002'



select
be.bag_type ,
    --distinct(be.bag_type),be.bag_number ,
    bm.administrative_flag
FROM
    bagmgmt.bag_event be
JOIN
    bagmgmt.bag_master bm ON bm.bag_type = be.bag_type --where be.bag_number ='LBT0013245643''CPB0000000822'
where --be.bag_type ='TB'
    be.bag_number = $1
ORDER BY
    be.transaction_date DESC
LIMIT 1;
select * from bagmgmt.bag_event where event_type in ('CL','DP') bag_number='CPB0000000822'
select * from bagmgmt.bag_close_content where bag_number='CPB0000000822'


SELECT
    bes.bag_number AS BagNumber,
    bes.transaction_date AS TranscationDate,
    om1.office_name AS FromOfficeName,
    om2.office_name AS ToOfficeName,
    bes.bag_type AS BagType,
    bes.delivery_type AS DeliveryType,
    bes.from_office_id AS FromOfficeID,
    bes.to_office_id AS ToOfficeID,
    bes.bag_weight AS BagWeight,
    bes.article_weight AS ArticleWeight,
    bes.article_count AS ArticleCount,
    bes.bag_count AS BagCount,
    bes.event_type AS EventType,
    bes.user_id AS UserID,
    bes.set_number AS SetNumber,
    bes.schedule_id AS ScheduleID,
    bes.set_date AS SetDate,
    jsonb_agg(json_build_object(
        'article_number', bccs.article_number,
        'article_type', bccs.article_type,
        'art_status', bccs.art_status,
        'insured_flag', bccs.insured_flag
    )) AS BagCloseArt
FROM
    bagmgmt.bag_event bes
JOIN
    bagmgmt.bag_close_content bccs ON bes.bag_number = bccs.bag_number
JOIN
    bagmgmt.kafka_office_master om1 ON bes.from_office_id = om1.office_id
JOIN
    bagmgmt.kafka_office_master om2 ON bes.to_office_id = om2.office_id
WHERE
   -- bes.bag_number = $1
    --AND 
    bes.event_type IN ('CL', 'DP')
GROUP BY
    bes.bag_number,
    bes.transaction_date,
    om1.office_name,
    om2.office_name,
    bes.bag_type,
    bes.delivery_type,
    bes.from_office_id,
    bes.to_office_id,
    bes.bag_weight,
    bes.article_weight,
    bes.article_count,
    bes.bag_count,
    bes.event_type,
    bes.user_id,
    bes.set_number,
    bes.schedule_id,
    bes.set_date -- Added bes.set_date to GROUP BY as it's in SELECT and not aggregated
ORDER BY
    bes.transaction_date DESC
LIMIT 1;