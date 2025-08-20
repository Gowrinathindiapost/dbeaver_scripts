SELECT 
		DATE(be.transaction_date) AS Date,
		TO_CHAR(be.transaction_date, 'HH24:MI:SS') AS Time,
		CASE 
			WHEN be.event_type IN ('CL','DI') THEN kom.office_name 
			WHEN be.event_type IN ('OP','OR') THEN kom2.office_name  
		END AS Office,
		CASE 
			WHEN be.event_type IN ('CL','DI') THEN be.from_office_id 
			WHEN be.event_type IN ('OP','OR') THEN be.to_office_id  
		END AS OfficeID,
		CASE 
			WHEN be.event_type = 'CL' THEN 'Item Bagged' 
			WHEN be.event_type IN('OP','OR') THEN 'Item Received' 
			WHEN be.event_type = 'DI' THEN 'Item Dispatched' 
		END AS Event
	FROM trackandtrace.kafka_bag_event be
	LEFT JOIN trackandtrace.kafka_office_master kom ON be.from_office_id = kom.office_id
	LEFT JOIN trackandtrace.kafka_office_master kom2 ON be.to_office_id = kom2.office_id
	WHERE be.bag_number IN (
		SELECT DISTINCT bag_number FROM trackandtrace.kafka_bag_close_content 
		UNION
		SELECT DISTINCT bag_number FROM trackandtrace.kafka_bag_open_content 
	)
	ORDER BY Date, Time;

INSERT INTO trackandtrace.bag_event_tracking (date, time, office, officeid, event)
SELECT 
	article_number,
    kae.event_date AS Date,
    if kae.event code =in  then item deliver,
    kae.event code =in then invoice,
    kae.event_code =RNT then return as event_type,
    
    if kae.event_code = in then ITEM_DELIVER as event_code,
    kae.current_office_id AS office_id,
    kom.office_name AS office_name,
    'kafka_article_event' as source_table,
    kae.remarks AS Event
FROM trackandtrace.kafka_article_event kae
JOIN trackandtrace.kafka_office_master kom 
    ON kae.current_office_id = kom.office_id
ORDER BY Date, Time;

select * from trackandtrace.kafka_article_event kae



SELECT CASE WHEN action_code = 1 THEN 'delivered' ELSE 'not delivered' END AS DelStat
	FROM trackandtrace.kafka_article_transaction 
	WHERE article_number = $1
	LIMIT 1;

