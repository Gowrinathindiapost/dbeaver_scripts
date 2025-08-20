
-- mis_db.mv_new_pdmanagement_article_transaction_tracking_event source


drop table mis_db.mv_new_pdmanagement_article_transaction_tracking_event

CREATE MATERIALIZED VIEW mis_db.mv_new_pdmanagement_article_transaction_tracking_event TO mis_db.new_customer_tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS SELECT DISTINCT
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
WHERE article_number IS NOT NULL;

-- mis_db.mv_ips_event_ooe_master_intl_tracking_event_new source

drop table mis_db.mv_ips_event_ooe_master_intl_tracking_event_new


CREATE MATERIALIZED VIEW mis_db.mv_ips_event_ooe_master_intl_tracking_event_new TO mis_db.new_customer_tracking_event_new_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS SELECT
    t1.article_number,

    t1.event_time AS event_date,

    t2.event_name AS event_type,

    t2.event_code AS event_code,

    '99999999' AS office_id,

    t3.ooe_name AS office_name,

    'ext_ips_ips_event' AS source_table,

    '' AS delivery_status,

    8 AS sort_order
FROM mis_db.ext_ips_ips_event AS t1
INNER JOIN mis_db.ext_ips_ips_event_master AS t2 ON t1.event_cd = t2.event_cd
INNER JOIN mis_db.dim_ips_ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd;
-- mis_db.mv_ips_event_ooe_master_intl_tracking_event source

drop table mis_db.mv_ips_event_ooe_master_intl_tracking_event

CREATE MATERIALIZED VIEW mis_db.mv_ips_event_ooe_master_intl_tracking_event TO mis_db.new_customer_tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS SELECT
    t1.article_number,

    t1.event_time AS event_date,

    t2.event_name AS event_type,

    t2.event_code AS event_code,

    '99999999' AS office_id,

    t3.ooe_name AS office_name,

    'ext_ips_ips_event' AS source_table,

    '' AS delivery_status,

    8 AS sort_order
FROM mis_db.ext_ips_ips_event AS t1
INNER JOIN mis_db.ext_ips_ips_event_master AS t2 ON t1.event_cd = t2.event_cd
INNER JOIN mis_db.dim_ips_ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd;
-- mis_db.mv_new_bag_events_tracking_event source

DROP TABLE mis_db.mv_new_bag_events_tracking_event

CREATE MATERIALIZED VIEW mis_db.mv_new_bag_events_tracking_event TO mis_db.new_customer_tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS SELECT DISTINCT
    coalesce(bcc.article_number,
 boc.article_number) AS article_number,

    be.transaction_date AS event_date,

    multiIf(be.event_type = 'CL',
 'Bag Close',
 be.event_type = 'DI',
 'Bag Dispatch',
 be.event_type = 'RO',
 'Item Received',
 be.event_type IN ('OP',
 'OR'),
 'Bag Open',
 '') AS event_type,

    multiIf(be.event_type = 'CL',
 'BAG_CLOSE',
 be.event_type = 'DI',
 'BAG_DISPATCH',
 be.event_type = 'RO',
 'TMO_RECEIVE',
 be.event_type IN ('OP',
 'OR'),
 'BAG_OPEN',
 '') AS event_code,

    coalesce(kom.csi_facility_id,
 kom2.csi_facility_id) AS office_id,

    coalesce(kom.office_name,
 kom2.office_name) AS office_name,

    'ext_bagmgmt_bag_event' AS source_table,

    '' AS delivery_status,

    3 AS sort_order
FROM mis_db.ext_bagmgmt_bag_event AS be
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content AS bcc ON (be.bag_number = bcc.bag_number) AND (be.event_type IN ('CL',
 'DI'))
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content AS boc ON (be.bag_number = boc.bag_number) AND (be.event_type IN ('OP',
 'OR'))
LEFT JOIN mis_db.ext_mdm_office_master AS kom ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 ON be.to_office_id = kom2.office_id;
-- mis_db.mv_new_article_events_tracking_event source

DROP TABLE mis_db.mv_new_article_events_tracking_event

CREATE MATERIALIZED VIEW mis_db.mv_new_article_events_tracking_event TO mis_db.new_customer_tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `event_code` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS SELECT
    kae.article_number,

    kae.event_date,

    multiIf(kae.event_code = 'RC',
 'Item Return',
 kae.event_code = 'ID',
 'Item delivery',
 kae.event_code = 'IN',
 'Item Bagging',
 kae.event_code = 'RT',
 'Item Return',
 kae.event_code = 'DE',
 'Item Onhold',
 kae.event_code = 'RD',
 'Item redirect',
 kae.event_code = 'IT',
 'Item Return',
 '') AS event_type,

    multiIf(kae.event_code = 'RC',
 'ITEM_RETURN',
 kae.event_code = 'ID',
 'ITEM_DELIVERY',
 kae.event_code = 'IN',
 'ITEM_BAGGING',
 kae.event_code = 'RT',
 'ITEM_RETURN',
 kae.event_code = 'DE',
 'ITEM_ONHOLD',
 kae.event_code = 'RD',
 'ITEM_REDIRECT',
 kae.event_code = 'IT',
 'ITEM_RETURN',
 '') AS event_code,

    kom.csi_facility_id AS office_id,

    kom.office_name,

    'ext_pdmanagement_article_event' AS source_table,

    '' AS delivery_status,

    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event AS kae
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kae.current_office_id = kom.office_id;