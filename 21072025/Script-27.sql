select * from trackandtrace.kafka_article_transaction kat where kat.article_number ='CY001745632IN'
select distinct(action_code),action_to_be_taken,remarks from trackandtrace.kafka_article_transaction kat
select * from trackandtrace.kafka_article_event kae where kae.article_number ='CY001745632IN'
29042025	154824	Kukkundur S.O	21280734	ITEM_RETURN	Item Return
SELECT 
    to_char(kat.created_date, 'DDMMYYYY') AS EventDate,
    to_char(kat.created_date, 'HH24MISS') AS EventTime,
    kat.source_office_name as EventOfficeName,
    kat.source_office_id as EventOfficeFaciltyID,
    CASE 
        WHEN kat.action_code = 1 THEN 'ITEM_DELIVERY'
        WHEN kat.action_code = 2 THEN 'ITEM_ONHOLD'
        WHEN kat.action_code = 3 THEN 'ITEM_REDIRECT'
        WHEN kat.action_code = 4 THEN 'ITEM_RETURN'
        WHEN kat.action_code = 5 THEN 'ITEM_ONHOLD'
        WHEN kat.action_code = 8 THEN 'ITEM_REDIRECT'
        WHEN kat.action_code = 3 THEN 'ITEM_REDIRECT'
        WHEN kat.action_code = 12 THEN 'ITEM_DELIVERY'
         WHEN kat.action_code = 303 THEN 'ITEM_REDIRECT'
        ELSE 'UNKNOWN'
    END AS EventCode,
    CASE 
        WHEN kat.action_code = 1 THEN 'Item delivery'
        WHEN kat.action_code = 2 THEN 'Item Onhold'
        WHEN kat.action_code = 3 THEN 'Item redirect'
        WHEN kat.action_code = 4 THEN 'Item Return'
        WHEN kat.action_code = 5 THEN 'Item Onhold'
        WHEN kat.action_code = 8 THEN 'Item redirect'
        WHEN kat.action_code = 12 THEN 'Item delivery'
        WHEN kat.action_code = 303 THEN 'Item redirect'
        ELSE 'Unknown Description'
    END AS EventDescription
FROM 
    trackandtrace.kafka_article_transaction kat

WHERE 
    kat.article_number = 'CY001745561IN'
ORDER BY 
    kat.created_date DESC
LIMIT 1;
