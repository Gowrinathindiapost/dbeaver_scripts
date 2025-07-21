SELECT
    formatDateTime(kat.created_date, '%d%m%Y') AS EventDate,
    formatDateTime(kat.created_date, '%H%i%s') AS EventTime,
    kat.source_office_name AS EventOfficeName,
    kat.source_office_id AS EventOfficeFaciltyID,

    CASE 
        WHEN kat.action_code = 1 THEN 'ITEM_DELIVERY'
        WHEN kat.action_code = 2 THEN 'ITEM_ONHOLD'
        WHEN kat.action_code = 3 THEN 'ITEM_REDIRECT'
        WHEN kat.action_code = 4 THEN 'ITEM_RETURN'
        WHEN kat.action_code = 5 THEN 'ITEM_ONHOLD'
        WHEN kat.action_code = 8 THEN 'ITEM_REDIRECT'
        WHEN kat.action_code = 12 THEN 'ITEM_DELIVERY'
        WHEN kat.action_code = 0 THEN 'ITEM_DELIVERY'
        WHEN kat.action_code = 303 THEN 'ITEM_REDIRECT'
    END AS EventCode,

    CASE 
        WHEN kat.action_code = 1 THEN 'Item delivery'
        WHEN kat.action_code = 2 THEN 'Item Onhold'
        WHEN kat.action_code = 3 THEN 'Item redirect'
        WHEN kat.action_code = 4 THEN 'Item Return'
        WHEN kat.action_code = 5 THEN 'Item Onhold'
        WHEN kat.action_code = 8 THEN 'Item redirect'
        WHEN kat.action_code = 12 THEN 'Item delivery'
        WHEN kat.action_code = 0 THEN 'Item delivery'
        WHEN kat.action_code = 303 THEN 'Item redirect'
    END AS EventDescription

FROM mis_db.ext_pdmanagement_article_transaction AS kat
WHERE kat.article_number = 'EK745892664IN'
ORDER BY kat.created_date DESC
LIMIT 1;

