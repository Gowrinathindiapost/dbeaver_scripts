--old 33 s
explain analyze WITH bagged AS (
	SELECT DISTINCT bag_number
	FROM trackandtrace.kafka_bag_close_content
	WHERE article_number = $1
),
received AS (
	SELECT DISTINCT bag_number
	FROM trackandtrace.kafka_bag_open_content
	WHERE article_number = $2
)
SELECT
	DATE(be.transaction_date) AS Date,
	to_char(be.transaction_date, 'HH24:MI:SS') AS Time,
	CASE
		WHEN be.event_type IN ('CL', 'DI') THEN kom.office_name
		WHEN be.event_type IN ('OP', 'OR') THEN kom2.office_name
		ELSE NULL
	END AS Office,
	CASE
		WHEN be.event_type = 'CL' THEN 'Item Bagged'
		WHEN be.event_type IN ('OP', 'OR') THEN 'Item Received'
		WHEN be.event_type = 'DI' THEN 'Item Dispatched'
	END AS Event
FROM trackandtrace.kafka_bag_event be
LEFT JOIN trackandtrace.kafka_office_master kom ON be.from_office_id = kom.office_id
LEFT JOIN trackandtrace.kafka_office_master kom2 ON be.to_office_id = kom2.office_id
LEFT JOIN bagged bcl ON be.bag_number = bcl.bag_number AND be.event_type IN ('CL', 'DI')
LEFT JOIN received bo ON be.bag_number = bo.bag_number AND be.event_type IN ('OP', 'OR')
WHERE
	(be.event_type IN ('CL', 'DI') AND bcl.bag_number IS NOT NULL)
	OR
	(be.event_type IN ('OP', 'OR') AND bo.bag_number IS NOT NULL)
ORDER BY Date, Time




----new 0.444ms
explain analyze WITH relevant_bags AS (
    SELECT bag_number FROM trackandtrace.kafka_bag_close_content WHERE article_number = $1
    UNION
    SELECT bag_number FROM trackandtrace.kafka_bag_open_content WHERE article_number = $1
)
SELECT
    DATE(be.transaction_date) AS Date,
    to_char(be.transaction_date, 'HH24:MI:SS') AS Time,
    CASE
        WHEN be.event_type IN ('CL', 'DI') THEN kom.office_name
        WHEN be.event_type IN ('OP', 'OR') THEN kom2.office_name
        ELSE NULL
    END AS Office,
    CASE
        WHEN be.event_type = 'CL' THEN 'Item Bagged'
        WHEN be.event_type IN ('OP', 'OR') THEN 'Item Received'
        WHEN be.event_type = 'DI' THEN 'Item Dispatched'
    END AS Event
FROM trackandtrace.kafka_bag_event be
JOIN relevant_bags rb ON be.bag_number = rb.bag_number
LEFT JOIN trackandtrace.kafka_office_master kom ON 
    be.event_type IN ('CL', 'DI') AND be.from_office_id = kom.office_id
LEFT JOIN trackandtrace.kafka_office_master kom2 ON 
    be.event_type IN ('OP', 'OR') AND be.to_office_id = kom2.office_id
WHERE be.event_type IN ('CL', 'DI', 'OP', 'OR')
ORDER BY Date, Time;

---- slow
explain analyze
WITH bag_numbers AS (
    SELECT DISTINCT bag_number FROM (
        SELECT bag_number FROM trackandtrace.kafka_bag_close_content WHERE article_number = $1
        UNION ALL
        SELECT bag_number FROM trackandtrace.kafka_bag_open_content WHERE article_number = $1
    ) t
)
SELECT
    DATE(be.transaction_date) AS Date,
    to_char(be.transaction_date, 'HH24:MI:SS') AS Time,
    CASE
        WHEN be.event_type IN ('CL', 'DI') THEN kom.office_name
        WHEN be.event_type IN ('OP', 'OR') THEN kom2.office_name
        ELSE NULL
    END AS Office,
    CASE
        WHEN be.event_type = 'CL' THEN 'Item Bagged'
        WHEN be.event_type IN ('OP', 'OR') THEN 'Item Received'
        WHEN be.event_type = 'DI' THEN 'Item Dispatched'
    END AS Event
FROM trackandtrace.kafka_bag_event be
JOIN bag_numbers bn ON be.bag_number = bn.bag_number
LEFT JOIN trackandtrace.kafka_office_master kom ON 
    be.event_type IN ('CL', 'DI') AND be.from_office_id = kom.office_id
LEFT JOIN trackandtrace.kafka_office_master kom2 ON 
    be.event_type IN ('OP', 'OR') AND be.to_office_id = kom2.office_id
WHERE be.event_type IN ('CL', 'DI', 'OP', 'OR')
ORDER BY Date, Time;
