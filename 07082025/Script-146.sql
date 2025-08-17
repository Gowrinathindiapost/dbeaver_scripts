SELECT
    DATE(event_date) AS Date,
    TO_CHAR(event_date, 'HH24:MI:SS') AS Time,
    kom.office_name AS Office,
    kae.remarks AS Event
FROM trackandtrace.kafka_article_event AS kae
JOIN trackandtrace.kafka_office_master AS kom
    ON kae.current_office_id = kom.office_id
where kae.event_code<>'RC' 
and kae.article_number = 'ZD126278647IN';
