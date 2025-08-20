SELECT DISTINCT
    article_number,

    now64(6) AS event_date,

    multiIf((action_code = 1) AND (epat.is_returned = false),
 'Item Delivered(Addressee)',
 (action_code = 1) AND (epat.is_returned = true),
 'Item Delivered(Sender)',
 'Not Delivered') AS event_type,

    multiIf(action_code = 1,
 'ITEM_DELIVERY',
 'ITEM_NONDELIVER') AS event_code,

    csi_facility_id AS office_id,

    current_office_name AS office_name,

    'ext_pdmanagement_article_transaction' AS source_table,

    multiIf(action_code = 1,
 '',
 epat.remarks) AS delivery_status,

    7 AS sort_order
FROM mis_db.ext_pdmanagement_article_transaction AS epat
INNER JOIN mis_db.ext_mdm_office_master AS kom ON epat.current_office_id = kom.office_id
WHERE article_number='MS010651200IN' IS NOT NULL;