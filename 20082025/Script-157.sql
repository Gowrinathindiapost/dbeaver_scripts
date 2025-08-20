SELECT kmd.article_number AS "ArticleNumber", kmd.origin_office_name AS "Bookedat", kmd.md_created_date AS "Bookedon",
			       kmd.destination_pincode AS "DestinationPincode", kcd.total_amount AS "Tariff",
			       kmd.mail_type_code AS "ArticleType", kmd.destination_office_name AS "DeliveryLocation",
			       kat.event_date AS "DeliveryConfirmedOn", 1 AS priority
			FROM trackandtrace.kafka_mailbooking_dom kmd
			LEFT JOIN trackandtrace.kafka_charges_detail kcd ON kmd.charges_detail_id = kcd.charges_detail_id
			LEFT JOIN trackandtrace.kafka_article_event kat ON kmd.article_number = kat.article_number AND kat.event_code = 'ID'
			WHERE kmd.article_number = 'AQ907269145IN'
			
			
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
		SELECT DISTINCT bag_number FROM trackandtrace.kafka_bag_close_content WHERE article_number = 'AQ907269145IN'
		UNION
		SELECT DISTINCT bag_number FROM trackandtrace.kafka_bag_open_content WHERE article_number = 'AQ907269145IN'
	)
	ORDER BY Date, Time;