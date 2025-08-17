
---------------------1
-- mis_db.mv_article_events_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_article_events_tracking_event TO mis_db.tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `office_id` Int32,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS 
insert into mis_db.tracking_event_mv
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
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kae.current_office_id = kom.office_id;

---------------2
-- mis_db.mv_bag_events_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_bag_events_tracking_event TO mis_db.tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `office_id` Int32,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS 
insert into mis_db.tracking_event_mv
SELECT
    coalesce(bcc.article_number,
 boc.article_number) AS article_number,

    be.transaction_date AS event_date,

    multiIf(be.event_type = 'CL',
 'Item Bagged',
 be.event_type = 'DI',
 'Item Dispatched',
 be.event_type IN ('OP',
 'OR'),
 'Item Received',
 '') AS event_type,

    coalesce(kom.office_id,
 kom2.office_id) AS office_id,

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
-----------------------------------------3

-- mis_db.mv_booking_dom_to_booking_dom_mv source

CREATE MATERIALIZED VIEW mis_db.mv_booking_dom_to_booking_dom_mv TO mis_db.mv_booking_dom
(

    `article_number` String,

    `bookedat` String,

    `bookedon` DateTime64(6),

    `destination_pincode` Int32,

    `delivery_location` String,

    `article_type` String,

    `tariff` Decimal(10,
 2)
)
AS 
--insert into mis_db.mv_booking_dom
SELECT
    kmd.article_number AS article_number,

    kmd.origin_office_name AS bookedat,

    kmd.md_created_date AS bookedon,

    kmd.destination_pincode AS destination_pincode,

    kmd.destination_office_name AS delivery_location,

    kmd.mail_type_code AS article_type,

    kcd.total_amount AS tariff
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);

------------4
-- mis_db.mv_pdmanagement_unregisteredarticle_delivered_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_pdmanagement_unregisteredarticle_delivered_tracking_event TO mis_db.tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `office_id` Int32,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS 
insert into mis_db.tracking_event_mv
SELECT
    article_number,

    remarks_date AS event_date,

    'Item Delivered' AS event_type,

    office_id,

    kom.office_name,

    'ext_pdmanagement_unregisteredarticle_dateentry' AS source_table,

    '' AS delivery_status,

    5 AS sort_order
FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
INNER JOIN mis_db.ext_mdm_office_master AS kom USING (office_id)
WHERE status = 2;

----------------------------5

-- mis_db.mv_pdmanagement_unregisteredarticle_dateentry_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_pdmanagement_unregisteredarticle_dateentry_tracking_event TO mis_db.tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `office_id` Int32,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS 
insert into mis_db.tracking_event_mv
SELECT
    article_number,

    invoice_date AS event_date,

    'Item Invoiced' AS event_type,

    office_id,

    kom.office_name,

    'ext_pdmanagement_unregisteredarticle_dateentry' AS source_table,

    '' AS delivery_status,

    5 AS sort_order
FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
INNER JOIN mis_db.ext_mdm_office_master AS kom USING (office_id)
WHERE invoice_date IS NOT NULL;

--------------6
-- mis_db.mv_pdmanagement_article_recall_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_pdmanagement_article_recall_tracking_event TO mis_db.tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `office_id` Int32,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS 
insert into mis_db.tracking_event_mv
SELECT
    karr.article_number AS article_number,

    karr.transaction_date AS event_date,

    'Item Recalled' AS event_type,

    kom.office_id,

    kom.office_name,

    'ext_pdmanagement_article_recall_return' AS source_table,

    '' AS delivery_status,

    6 AS sort_order
FROM mis_db.ext_pdmanagement_article_recall_return AS karr
INNER JOIN mis_db.ext_mailbkg_mailbooking_dom AS kmd ON karr.article_number = kmd.article_number
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
UNION ALL
SELECT
    karr.article_number AS article_number,

    karr.transaction_date AS event_date,

    'Item Recalled' AS event_type,

    kom.office_id,

    kom.office_name,

    'ext_pdmanagement_article_recall_return' AS source_table,

    '' AS delivery_status,

    6 AS sort_order
FROM mis_db.ext_pdmanagement_article_recall_return AS karr
INNER JOIN mis_db.ext_mailbkg_mailbooking_intl AS kmd ON karr.article_number = kmd.article_number
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id;
------------7
-- mis_db.mv_ips_event_ooe_master_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_ips_event_ooe_master_tracking_event TO mis_db.tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS 

insert into mis_db.tracking_event_mv
SELECT
    t1.article_number,

    t1.event_time AS event_date,

    t2.event_name AS event_type,

    '99999999' AS office_id,

    t3.ooe_name AS office_name,

    'ext_ips_ips_event' AS source_table,

    '' AS delivery_status,

    8 AS sort_order
FROM mis_db.ext_ips_ips_event AS t1
INNER JOIN mis_db.ext_ips_ips_event_master AS t2 ON t1.event_cd = t2.event_cd
INNER JOIN mis_db.dim_ips_ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd;

----------------------8
