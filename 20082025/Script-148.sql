CREATE TABLE bag_event_tracking (
    article_number     TEXT,
    event_date         TIMESTAMP,
    event_type         TEXT,
    event_code         TEXT,
    office_id          TEXT,
    office_name        TEXT,
    source_table       TEXT,
    delivery_status    TEXT,
    sort_order         INTEGER
);

select * from article_bag_tracking
drop table bag_event_tracking
TRUNCATE TABLE article_bag_tracking;

select * from bag_event_tracking
INSERT INTO bag_event_tracking (
    article_number,
    event_date,
    event_type,
    event_code,
    office_id,
    office_name,
    source_table,
    delivery_status,
    sort_order
)
SELECT DISTINCT
    COALESCE(bcc.article_number, boc.article_number) AS article_number,
    be.transaction_date AS event_date,
    CASE 
        WHEN be.event_type = 'CL' THEN 'Bag Close'
        WHEN be.event_type = 'DI' THEN 'Bag Dispatch'
        WHEN be.event_type = 'RO' THEN 'Item Received'
        WHEN be.event_type IN ('OP', 'OR') THEN 'Bag Open'
        ELSE ''
    END AS event_type,
    CASE 
        WHEN be.event_type = 'CL' THEN 'BAG_CLOSE'
        WHEN be.event_type = 'DI' THEN 'BAG_DISPATCH'
        WHEN be.event_type = 'RO' THEN 'TMO_RECEIVE'
        WHEN be.event_type IN ('OP', 'OR') THEN 'BAG_OPEN'
        ELSE ''
    END AS event_code,
    COALESCE(kom.csi_facility_id, kom2.csi_facility_id) AS office_id,
    COALESCE(kom.office_name, kom2.office_name) AS office_name,
    'ext_bagmgmt_bag_event' AS source_table,
    '' AS delivery_status,
    3 AS sort_order
FROM kafka_bag_event AS be
LEFT JOIN kafka_bag_close_content AS bcc 
    ON be.bag_number = bcc.bag_number AND be.event_type IN ('CL', 'DI')
LEFT JOIN kafka_bag_open_content AS boc 
    ON be.bag_number = boc.bag_number AND be.event_type IN ('OP', 'OR')
LEFT JOIN kafka_office_master AS kom 
    ON be.from_office_id = kom.office_id
LEFT JOIN kafka_office_master AS kom2 
    ON be.to_office_id = kom2.office_id;

SELECT 
		DATE(kae.event_date) AS Date,
		TO_CHAR(kae.event_date, 'HH24:MI:SS') AS Time,
		kom.office_name AS Office,
		kae.current_office_id AS OfficeID,
		kae.remarks AS Event
	FROM trackandtrace.kafka_article_event kae
	JOIN trackandtrace.kafka_office_master kom ON kae.current_office_id = kom.office_id

	ORDER BY Date, Time;


	
	INSERT INTO bag_event_tracking (
    article_number,
    event_date,
    event_type,
    event_code,
    office_id,
    office_name,
    source_table,
    delivery_status,
    sort_order
)
SELECT 
    kae.article_number,
    kae.event_date,
    kae.remarks AS event_type,
    'ARTICLE_EVENT' AS event_code,
    kae.current_office_id::TEXT AS office_id,
    kom.office_name,
    'kafka_article_event' AS source_table,
    '' AS delivery_status,
    4 AS sort_order
FROM trackandtrace.kafka_article_event kae
JOIN trackandtrace.kafka_office_master kom 
    ON kae.current_office_id = kom.office_id
WHERE kae.article_number = $1
ORDER BY kae.event_date;
select * from kafka_article_event