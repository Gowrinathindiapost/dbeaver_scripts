


-- Index for WHERE clause
CREATE INDEX idx_kae_article_number ON trackandtrace.kafka_article_event(article_number);--already available 07082025

-- Index for JOIN column on kafka_article_event
CREATE INDEX idx_kae_current_office_id ON trackandtrace.kafka_article_event(current_office_id);--already available 07082025

-- Index for JOIN column on kafka_office_master
CREATE INDEX idx_kom_office_id ON trackandtrace.kafka_office_master(office_id);--already available 07082025


CREATE INDEX idx_kae_article_office ON trackandtrace.kafka_article_event(article_number, current_office_id);

explain analyze
SELECT 
	kae.article_number ,
    DATE(kae.event_date) AS Date,
    TO_CHAR(kae.event_date, 'HH24:MI:SS') AS Time,
    kom.office_name AS Office,
    kae.remarks AS Event
FROM 
    trackandtrace.kafka_article_event kae
JOIN 
    trackandtrace.kafka_office_master kom 
    ON kae.current_office_id = kom.office_id
WHERE 
    kae.article_number = 'RA977476762IN';

explain analyze 
WITH bagged AS (
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



CREATE INDEX idx_bag_close_article_number 
ON trackandtrace.kafka_bag_close_content(article_number);--available


CREATE INDEX idx_bag_open_article_number 
ON trackandtrace.kafka_bag_open_content(article_number);--available


CREATE INDEX idx_bag_close_bag_number 
ON trackandtrace.kafka_bag_close_content(bag_number);--available


CREATE INDEX idx_bag_open_bag_number 
ON trackandtrace.kafka_bag_open_content(bag_number);--not available


CREATE INDEX idx_bag_event_bag_number_event_type 
ON trackandtrace.kafka_bag_event(bag_number, event_type);--not available


CREATE INDEX idx_bag_event_transaction_date 
ON trackandtrace.kafka_bag_event(transaction_date);--not available


CREATE INDEX idx_bag_event_from_office_id 
ON trackandtrace.kafka_bag_event(from_office_id);--available

CREATE INDEX idx_bag_event_to_office_id 
ON trackandtrace.kafka_bag_event(to_office_id);--available


----per
CREATE INDEX idx_bag_close_content_article_number
ON trackandtrace.kafka_bag_close_content (article_number, bag_number);

CREATE INDEX idx_bag_open_content_article_number
ON trackandtrace.kafka_bag_open_content (article_number, bag_number);
