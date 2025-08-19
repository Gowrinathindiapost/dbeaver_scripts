WITH bagged AS (
	SELECT DISTINCT bag_number
	FROM trackandtrace.kafka_bag_close_content
	WHERE article_number = 'RT874671416IN'
),
received AS (
	SELECT DISTINCT bag_number
	FROM trackandtrace.kafka_bag_open_content
	WHERE article_number = 'RT874671416IN'
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



SELECT
    DATE(be.transaction_date) AS Date,
    to_char(be.transaction_date, 'HH24:MI:SS') AS Time,
    CASE
        WHEN be.event_type IN ('CL', 'DI') THEN kom.office_name
        WHEN be.event_type IN ('OP', 'OR') THEN kom2.office_name
    END AS Office,
    CASE be.event_type
        WHEN 'CL' THEN 'Item Bagged'
        WHEN 'OP' THEN 'Item Received'
        WHEN 'OR' THEN 'Item Received'
        WHEN 'DI' THEN 'Item Dispatched'
    END AS Event
FROM trackandtrace.kafka_bag_event be
LEFT JOIN trackandtrace.kafka_office_master kom 
    ON be.from_office_id = kom.office_id
LEFT JOIN trackandtrace.kafka_office_master kom2 
    ON be.to_office_id = kom2.office_id
WHERE
    (
        be.event_type IN ('CL', 'DI') 
        AND EXISTS (
            SELECT 1
            FROM trackandtrace.kafka_bag_close_content c
            WHERE c.article_number = 'RT874671416IN'
              AND c.bag_number = be.bag_number
        )
    )
    OR
    (
        be.event_type IN ('OP', 'OR') 
        AND EXISTS (
            SELECT 1
            FROM trackandtrace.kafka_bag_open_content o
            WHERE o.article_number = 'RT874671416IN'
              AND o.bag_number = be.bag_number
        )
    )
ORDER BY Date, Time;
