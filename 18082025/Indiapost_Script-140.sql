select count(*),article_number from mis_db.tracking_event_mv GROUP by (article_number)
DROP TABLE mis_db.mv_booking_dom ON CLUSTER cluster_1S_2R
optimize TABLE mis_db.mv_booking_dom ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.mv_booking_intl ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.tracking_event_mv ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.new_customer_tracking_event_new_mv ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.new_customer_xml_facility_customer_new_mv ON CLUSTER cluster_1S_2R;





----------------------------14 target table
-- mis_db.mv_booking_dom definition

CREATE TABLE mis_db.mv_booking_dom ON CLUSTER cluster_1S_2R
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
ENGINE = ReplicatedReplacingMergeTree(bookedon)
PRIMARY KEY (article_number, bookedon)
ORDER BY (article_number, bookedon)
SETTINGS index_granularity = 8192;


---------------------------------
DROP TABLE mis_db.mv_booking_intl ON CLUSTER cluster_1S_2R
--------------------13 target table
-- mis_db.mv_booking_intl definition

CREATE TABLE mis_db.mv_booking_intl ON CLUSTER cluster_1S_2R
(

    `article_number` String,

    `bookedat` String,

    `bookedon` DateTime64(6),

    `source_country` String,

    `destination_country` String,

    `delivery_location` String,

    `article_type` String,

    `tariff` Decimal(10,
 2)
)
ENGINE = ReplicatedReplacingMergeTree(bookedon)
PRIMARY KEY (article_number, bookedon)
ORDER BY (article_number, bookedon)
SETTINGS index_granularity = 8192;
---------------------------------------
DROP TABLE mis_db.mv_booking_intl_to_booking_intl_mv ON CLUSTER cluster_1S_2R

---------------12 MV
-- mis_db.mv_booking_intl_to_booking_intl_mv source

CREATE MATERIALIZED VIEW mis_db.mv_booking_intl_to_booking_intl_mv TO mis_db.mv_booking_intl
(

    `article_number` String,

    `bookedat` String,

    `bookedon` DateTime64(6),

    `source_country` String,

    `destination_country` String,

    `delivery_location` String,

    `article_type` String,

    `tariff` Decimal(10,
 2)
)
AS 
--insert into mis_db.mv_booking_intl
SELECT
    kmd.article_number AS article_number,

    kom.office_name AS bookedat,

    kmd.md_updated_date AS bookedon,

    kmd.sender_country_name AS source_country,

    kmd.receiver_country_name AS destination_country,

    kmd.receiver_country_name AS delivery_location,

    kmd.mail_type_code AS article_type,

    kcd.total_amount AS tariff
FROM mis_db.ext_mailbkg_mailbooking_intl AS kmd
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);
------------------------
DROP TABLE mis_db.mv_booking_dom_to_booking_dom_mv ON CLUSTER cluster_1S_2R
--------------------11 MV
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
insert into mis_db.mv_booking_dom
SELECT
    kmd.article_number AS article_number,

    kmd.origin_office_name AS bookedat,

    kmd.md_updated_date AS bookedon,

    kmd.destination_pincode AS destination_pincode,

    kmd.destination_office_name AS delivery_location,

    kmd.mail_type_code AS article_type,

    kcd.total_amount AS tariff
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0)
AND kmd.md_updated_date >= '2025-08-01'
---------------------------------
DROP TABLE mis_db.tracking_event_mv ON CLUSTER cluster_1S_2R
---------------10 target table
-- mis_db.tracking_event_mv definition

CREATE TABLE mis_db.tracking_event_mv ON CLUSTER cluster_1S_2R
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
ENGINE = ReplicatedReplacingMergeTree(event_date)
PRIMARY KEY (article_number, event_date)
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;

--------------------------------------
DROP TABLE mis_db.mv_pdmanagement_unregisteredarticle_delivered_tracking_event ON CLUSTER cluster_1S_2R
-------------------9 MV
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
--insert into mis_db.tracking_event_mv
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
---------------------------------------
DROP TABLE mis_db.mv_pdmanagement_unregisteredarticle_dateentry_tracking_event ON CLUSTER cluster_1S_2R
--------------------8 MV
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
--insert into mis_db.tracking_event_mv
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
----------------------------------------
DROP TABLE mis_db.mv_pdmanagement_article_recall_tracking_event ON CLUSTER cluster_1S_2R

------------------7 MV
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
--insert into mis_db.tracking_event_mv
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
-------------------------------
DROP TABLE mis_db.mv_ips_event_tracking_event_mv ON CLUSTER cluster_1S_2R
---------------------6 MV
-- mis_db.mv_ips_event_tracking_event_mv source

CREATE MATERIALIZED VIEW mis_db.mv_ips_event_tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `office_id` String,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` String
)
ENGINE = MergeTree
ORDER BY article_number
SETTINGS index_granularity = 8192
AS SELECT
    t1.article_number,

    t1.event_time AS event_date,

    t2.event_name AS event_type,

    '99999999' AS office_id,

    t3.ooe_name AS office_name,

    'ext_ips_ips_event' AS source_table,

    '' AS delivery_status,

    '' AS sort_order
FROM mis_db.ext_ips_ips_event AS t1
INNER JOIN mis_db.ext_ips_ips_event_master AS t2 ON t1.event_cd = t2.event_cd
INNER JOIN mis_db.dim_ips_ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd;
-----------------------------
DROP TABLE mis_db.mv_ips_event_ooe_master_tracking_event ON CLUSTER cluster_1S_2R
DROP TABLE mis_db.mv_ips_event_tracking_event ON CLUSTER cluster_1S_2R
--------------------------5 MV
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
-----------------------------------

DROP TABLE mis_db.mv_booking_intl_tracking_event ON CLUSTER cluster_1S_2R
---------------------------4 MV
-- mis_db.mv_booking_intl_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_booking_intl_tracking_event TO mis_db.tracking_event_mv
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
    kmd.article_number,

    kmd.md_updated_date AS event_date,

    'Item Booked' AS event_type,

    kom.office_id AS office_id,

    kom.office_name AS office_name,

    'ext_mailbkg_mailbooking_intl' AS source_table,

    '' AS delivery_status,

    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_intl AS kmd
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);
-----------------------------------------------------------
DROP TABLE mis_db.mv_booking_dom_to_tracking_event ON CLUSTER cluster_1S_2R
------------------------3 MV
-- mis_db.mv_booking_dom_to_tracking_event source

CREATE MATERIALIZED VIEW mis_db.mv_booking_dom_to_tracking_event TO mis_db.tracking_event_mv
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
    kmd.article_number AS article_number,

    kmd.md_updated_date AS event_date,

    'Item Booked' AS event_type,

    kom.office_id AS office_id,

    kom.office_name AS office_name,

    'ext_mailbkg_mailbooking_dom' AS source_table,

    '' AS delivery_status,

    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);
-----------------------------------

DROP TABLE mis_db.mv_bag_events_tracking_event ON CLUSTER cluster_1S_2R
----------------2 MV
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
---------------------------------
DROP TABLE mis_db.mv_article_events_tracking_event ON CLUSTER cluster_1S_2R
--------1 MV
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