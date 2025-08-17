SELECT be.ml_id,be.schedule_id,be.from_office_id,be.to_office_id,be.user_id,be.set_number,be.bag_type --,sum(),sum(),ml.created_date,be.bag_number,be.bag_weight
FROM bagmgmt.bag_event be order by bag_event_id desc
--where be.ml_id =54439
join bagmgmt.mail_list ml on be.ml_id =ml.mail_id
WHERE be.event_type = 'DI' and be.ml_id = 54439

ORDER BY bag_event_id DESC;


--------------------------------------
SELECT 
    be.ml_id,
    be.schedule_id,
    be.from_office_id,
    be.to_office_id,
    be.user_id,
    be.set_number,
    be.bag_type,
    COUNT(*) AS total_bags,
    SUM(be.bag_weight) AS total_weight,
    ml.created_date,
    be.bag_number,
    be.bag_weight
FROM bagmgmt.bag_event be
JOIN bagmgmt.mail_list ml ON be.ml_id = ml.mail_id
WHERE be.event_type = 'DI' AND be.ml_id = 54439
GROUP BY 
be.bag_event_id ,
    be.ml_id,
    be.schedule_id,
    be.from_office_id,
    be.to_office_id,
    be.user_id,
    be.set_number,
    be.bag_type,
    ml.created_date,
    be.bag_number,
    be.bag_weight
ORDER BY be.bag_event_id DESC;
-----------------------------------------------------
SELECT 
    be.ml_id,
    be.schedule_id,
    be.from_office_id,
    be.to_office_id,
    be.user_id,
    be.set_number,
    be.bag_type,
    COUNT(*) OVER (PARTITION BY be.ml_id) AS total_bags,
    SUM(be.bag_weight) OVER (PARTITION BY be.ml_id) AS total_weight,
    ml.created_date,
    be.bag_number,
    be.bag_weight
FROM bagmgmt.bag_event be
JOIN bagmgmt.mail_list ml ON be.ml_id = ml.mail_id
WHERE be.event_type = 'DI' --and ml.created_date >= time.now -5 min  --AND be.ml_id = 53675 --54439
 AND ml.created_date >= NOW() - INTERVAL '5000 minutes'
ORDER BY be.bag_event_id DESC;

