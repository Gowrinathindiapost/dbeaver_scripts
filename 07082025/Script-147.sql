SELECT
    kae.article_number,

    kae.event_date,

    kae.remarks AS event_type,

    kom.office_id,

    kom.office_name,

    'ext_pdmanagement_article_event' AS source_table,

    '' AS delivery_status,

    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event AS kae
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kae.current_office_id = kom.office_id
Where kae.event_code<>'RC'