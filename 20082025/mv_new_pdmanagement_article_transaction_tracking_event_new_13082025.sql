-- mis_db.mv_new_pdmanagement_article_transaction_tracking_event_new source

DROP TABLE mis_db.mv_new_pdmanagement_article_transaction_tracking_event_new ON CLUSTER cluster_1S_2R

CREATE MATERIALIZED VIEW mis_db.mv_new_pdmanagement_article_transaction_tracking_event_new TO mis_db.new_customer_tracking_event_new_mv
(

    `article_number` String,

    `event_date` DateTime,

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS 
--below is old
--SELECT
--    article_number,
--
--    now() AS event_date,
--
--    multiIf(action_code = 1,
-- 'Item Delivery',
-- 'Not Delivered') AS event_type,
--
--    multiIf(action_code = 1,
-- 'ITEM_DELIVERY',
-- 'ITEM_NONDELIVER') AS event_code,
--
--    csi_facility_id AS office_id,
--
--    current_office_name AS office_name,
--
--    'ext_pdmanagement_article_transaction' AS source_table,
--
--    multiIf(action_code = 1,
-- '',
-- epat.remarks) AS delivery_status,
--
--    7 AS sort_order
--FROM mis_db.ext_pdmanagement_article_transaction AS epat
--INNER JOIN mis_db.ext_mdm_office_master AS kom ON epat.current_office_id = kom.office_id
--WHERE article_number IS NOT NULL;


--below is new one 
SELECT 
--DISTINCT
    article_number,
   -- now64(6) AS event_date,
    now() AS event_date,

    multiIf(
        action_code = 1 AND epat.is_returned = false, 'Item Delivered(Addressee)',
        action_code = 1 AND epat.is_returned = true,  'Item Delivered(Sender)',
        'Not Delivered'
    ) AS event_type,

--    multiIf(
--        action_code = 1 AND epat.is_returned = false, 'ITEM_DELIVERY_RECEIVER',
--        action_code = 1 AND epat.is_returned = true,  'ITEM_DELIVERY_SENDER',
--        'ITEM_NONDELIVER'
--    ) AS event_code,
        multiIf(action_code = 1,
 'ITEM_DELIVERY',
 'ITEM_NONDELIVER') AS event_code,

    csi_facility_id AS office_id,
    current_office_name AS office_name,
    'ext_pdmanagement_article_transaction' AS source_table,

--    multiIf(
--        action_code = 1 AND epat.is_returned = false, '',
--        action_code = 1 AND epat.is_returned = true,  '',
--        epat.remarks
--    ) AS delivery_status,
    
        multiIf(action_code = 1, '', epat.remarks) AS delivery_status,

    7 AS sort_order
FROM mis_db.ext_pdmanagement_article_transaction AS epat
INNER JOIN mis_db.ext_mdm_office_master AS kom 
    ON epat.current_office_id = kom.office_id
WHERE article_number IS NOT NULL