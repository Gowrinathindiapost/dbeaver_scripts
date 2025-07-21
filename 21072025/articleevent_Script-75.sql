SELECT
    formatDateTime(kae.event_date, '%d%m%Y') AS EventDate,
    formatDateTime(kae.event_date, '%H%i%s') AS EventTime,
    kom.office_name AS EventOfficeName,
    kae.current_office_id AS EventOfficeFaciltyID,
    CASE 
        WHEN kae.event_code = 'RC' THEN 'ITEM_RETURN'
        WHEN kae.event_code = 'ID' THEN 'ITEM_DELIVERY'
        WHEN kae.event_code = 'IN' THEN 'ITEM_BAGGING'
        WHEN kae.event_code = 'RT' THEN 'ITEM_RETURN'
        WHEN kae.event_code = 'DE' THEN 'ITEM_ONHOLD'
        WHEN kae.event_code = 'RD' THEN 'ITEM_REDIRECT'
        WHEN kae.event_code = 'IT' THEN 'ITEM_RETURN'
    END AS EventCode,
    CASE 
        WHEN kae.event_code = 'RC' THEN 'Item Return'
        WHEN kae.event_code = 'ID' THEN 'Item delivery'
        WHEN kae.event_code = 'IN' THEN 'Item Bagging'
        WHEN kae.event_code = 'RT' THEN 'Item Return'
        WHEN kae.event_code = 'DE' THEN 'Item Onhold'
        WHEN kae.event_code = 'RD' THEN 'Item redirect'
        WHEN kae.event_code = 'IT' THEN 'Item Return'
    END AS EventDescription
FROM mis_db.ext_pdmanagement_article_event AS kae
JOIN mis_db.ext_mdm_office_master AS kom
    ON kae.current_office_id = kom.office_id
WHERE kae.article_number = 'CK064583176IN'
ORDER BY kae.event_date DESC
LIMIT 1;
