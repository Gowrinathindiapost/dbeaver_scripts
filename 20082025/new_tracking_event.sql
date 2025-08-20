drop table mis_db.new_customer_tracking_event_mv
select * from mis_db.new_customer_tracking_event_mv
-------1
CREATE TABLE IF NOT EXISTS mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R
(
    article_number String,
    event_date DateTime,
    event_type String,
    event_code String,
    office_id String,
    office_name String,
    source_table String,
    delivery_status String,
    sort_order UInt8
)
ENGINE = ReplicatedMergeTree
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;
-- mis_db.new_customer_tracking_event_mv definition

CREATE TABLE mis_db.new_customer_tracking_event_mv
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
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
 '{replica}')
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;

CREATE TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R
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
ENGINE = ReplicatedMergeTree
ORDER BY (article_number,
 event_date)
SETTINGS index_granularity = 8192;
-----------2

INSERT INTO mis_db.new_customer_tracking_event_mv
SELECT
    kmd.article_number,
    kmd.md_updated_date AS event_date,
    'Item Book' AS event_type,
    'ITEM_BOOK' AS event_code,
    kom.csi_facility_id AS office_id,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_dom' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_dom kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.status_code = 'PC'

OPTIMIZE TABLE mis_db.new_customer_tracking_event_mv FINAL;
select * from mis_db.ext_mailbkg_mailbooking_dom kmd
select * from mis_db.ext_mdm_office_master where office_id='21660689'--PC21104000650

--INSERT INTO mis_db.tracking_event_mv
--SELECT
--    kmd.article_number,
--    kmd.md_updated_date,
--    'Item Booked',
--    kom.office_id,
--    kom.office_name ,
--    'ext_mailbkg_mailbooking_dom' ,
--    '' AS delivery_status,
--    1 AS sort_order
--FROM mis_db.ext_mailbkg_mailbooking_dom kmd
--JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id

drop table mis_db.mv_new_booking_dom_to_tracking_event ON CLUSTER cluster_1S_2R
CREATE MATERIALIZED VIEW mis_db.mv_new_booking_dom_to_tracking_event ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_tracking_event_mv
AS
INSERT into mis_db.new_customer_tracking_event_mv
SELECT
    kmd.article_number AS article_number,
     kmd.md_updated_date AS event_date,
    'Item Book' AS event_type,
    'ITEM_BOOK' AS event_code,
    --kom.office_id AS office_id,
    kom.csi_facility_id AS office_id,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_dom' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_dom kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.status_code = 'PC' AND
kmd._peerdb_is_deleted = 0;
select * from mis_db.mv_new_booking_dom_to_tracking_event

-- mis_db.mv_new_bag_events_tracking_event source

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
AS 
insert into mis_db.new_customer_tracking_event_mv
SELECT
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
---------4

select * from tracking_event_mv tem where source_table='ext_bagmgmt_bag_event'  and article_number like '%IN'
INSERT INTO mis_db.tracking_event_mv
SELECT COALESCE(bcc.article_number, boc.article_number), be.transaction_date,
       CASE WHEN be.event_type = 'CL' THEN 'Bag Close'
            WHEN be.event_type = 'DI' THEN 'Bag Dispatch'
            WHEN be.event_type = 'RO' THEN 'Item Received'
            WHEN be.event_type IN ('OP', 'OR') THEN 'Bag Open' END AS event_type,
        CASE WHEN be.event_type = 'CL' THEN 'BAG_CLOSE'
            WHEN be.event_type = 'DI' THEN 'BAG_DISPATCH'
            WHEN be.event_type = 'RO' THEN 'TMO_RECEIVE'
            WHEN be.event_type IN ('OP', 'OR') THEN 'BAG_OPEN' END AS event_code,
       COALESCE(kom.office_id, kom2.office_id), COALESCE(kom.office_name, kom2.office_name),
       'ext_bagmgmt_bag_event' AS source_table, 
       '' AS delivery_status, --non delivery reason
       3 AS sort_order
       
FROM mis_db.ext_bagmgmt_bag_event be
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content bcc ON be.bag_number = bcc.bag_number AND be.event_type IN ('CL', 'DI')
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content boc ON be.bag_number = boc.bag_number AND be.event_type IN ('OP', 'OR')
LEFT JOIN mis_db.ext_mdm_office_master kom ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master kom2 ON be.to_office_id = kom2.office_id 
where bcc.article_number ='EZ771854298IN' 'EK386633306IN' 

select * from mis_db.ext_bagmgmt_bag_event
drop table mis_db.mv_new_bag_events_tracking_event
CREATE MATERIALIZED VIEW mis_db.mv_new_bag_events_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_tracking_event_mv
AS
--insert into mis_db.new_customer_tracking_event_mv
SELECT 
    COALESCE(bcc.article_number, boc.article_number) AS article_number,
    be.transaction_date AS event_date,
--    CASE 
--        WHEN be.event_type = 'CL' THEN 'Item Bagged'
--        WHEN be.event_type = 'DI' THEN 'Item Dispatched'
--        WHEN be.event_type IN ('OP', 'OR') THEN 'Item Received'
--    END AS event_type,
--    be.event_type as event_code,
--    COALESCE(kom.office_id, kom2.office_id) AS office_id,
--    COALESCE(kom.office_name, kom2.office_name) AS office_name,
--    'ext_bagmgmt_bag_event' AS source_table,
--    '' AS delivery_status,
--    3 AS sort_order
     CASE WHEN be.event_type = 'CL' THEN 'Bag Close'
            WHEN be.event_type = 'DI' THEN 'Bag Dispatch'
            WHEN be.event_type = 'RO' THEN 'Item Received'
            WHEN be.event_type IN ('OP', 'OR') THEN 'Bag Open' END AS event_type,
        CASE WHEN be.event_type = 'CL' THEN 'BAG_CLOSE'
            WHEN be.event_type = 'DI' THEN 'BAG_DISPATCH'
            WHEN be.event_type = 'RO' THEN 'TMO_RECEIVE'
            WHEN be.event_type IN ('OP', 'OR') THEN 'BAG_OPEN' END AS event_code,
       COALESCE(kom.office_id, kom2.office_id) as office_id, 
       COALESCE(kom.office_name, kom2.office_name) as office_name,
       'ext_bagmgmt_bag_event' AS source_table, 
       '' AS delivery_status, --non delivery reason
       3 AS sort_order
FROM mis_db.ext_bagmgmt_bag_event AS be
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content AS bcc 
    ON be.bag_number = bcc.bag_number AND be.event_type IN ('CL', 'DI')
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content AS boc 
    ON be.bag_number = boc.bag_number AND be.event_type IN ('OP', 'OR')
LEFT JOIN mis_db.ext_mdm_office_master AS kom 
    ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 
    ON be.to_office_id = kom2.office_id 
    where bcc.article_number='EZ771854298IN'

    select * from mis_db.ext_bagmgmt_bag_event

-----------------6
INSERT INTO trackandtrace.tracking_event_mv
SELECT kae.article_number, kae.event_date, kae.remarks, kom.office_id, kom.office_name,
       'kafka_article_event', '', 4
FROM trackandtrace.kafka_article_event kae
JOIN trackandtrace.kafka_office_master kom ON kae.current_office_id = kom.office_id
WHERE kae.event_date >= TIMESTAMP '2025-06-19 00:00:00';

INSERT INTO mis_db.tracking_event_mv
SELECT
    kae.article_number,
    kae.event_date,
    --kae.remarks AS event_type,
     CASE 
        WHEN kae.event_code = 'RC' THEN 'Item Return'
        WHEN kae.event_code = 'ID' THEN 'Item delivery'
        WHEN kae.event_code = 'IN' THEN 'Item Bagging'
        WHEN kae.event_code = 'RT' THEN 'Item Return'
        WHEN kae.event_code = 'DE' THEN 'Item Onhold'
         WHEN kae.event_code = 'RD' THEN 'Item redirect'
         WHEN kae.event_code = 'IT' THEN 'Item Return'
        
    END AS event_type,
    CASE 
        WHEN kae.event_code = 'RC' THEN 'ITEM_RETURN'
        WHEN kae.event_code = 'ID' THEN 'ITEM_DELIVERY'
        WHEN kae.event_code = 'IN' THEN 'ITEM_BAGGING'
        WHEN kae.event_code = 'RT' THEN 'ITEM_RETURN'
        WHEN kae.event_code = 'DE' THEN 'ITEM_ONHOLD'
         WHEN kae.event_code = 'RD' THEN 'ITEM_REDIRECT'
         WHEN kae.event_code = 'IT' THEN 'ITEM_RETURN'
        
    END AS event_code,
    
    kom.office_id,
    kom.office_name,
    'ext_pdmanagement_article_event' AS source_table,
    '' AS delivery_status,
    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event kae
JOIN mis_db.ext_mdm_office_master kom ON kae.current_office_id = kom.office_id
select * from mis_db.ext_pdmanagement_article_event kae where event_code <> ''
select * from mis_db.ext_pdmanagement_article_event kae where kae.event_code='IT'

CREATE MATERIALIZED VIEW mis_db.mv_new_article_events_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_tracking_event_mv
AS
--INSERT into mis_db.new_customer_tracking_event_mv
SELECT
    kae.article_number,
    kae.event_date,
--    kae.remarks AS event_type,
   CASE 
        WHEN kae.event_code = 'RC' THEN 'Item Return'
        WHEN kae.event_code = 'ID' THEN 'Item delivery'
        WHEN kae.event_code = 'IN' THEN 'Item Bagging'
        WHEN kae.event_code = 'RT' THEN 'Item Return'
        WHEN kae.event_code = 'DE' THEN 'Item Onhold'
         WHEN kae.event_code = 'RD' THEN 'Item redirect'
         WHEN kae.event_code = 'IT' THEN 'Item Return'
        
    END AS event_type,
    CASE 
        WHEN kae.event_code = 'RC' THEN 'ITEM_RETURN'
        WHEN kae.event_code = 'ID' THEN 'ITEM_DELIVERY'
        WHEN kae.event_code = 'IN' THEN 'ITEM_BAGGING'
        WHEN kae.event_code = 'RT' THEN 'ITEM_RETURN'
        WHEN kae.event_code = 'DE' THEN 'ITEM_ONHOLD'
         WHEN kae.event_code = 'RD' THEN 'ITEM_REDIRECT'
         WHEN kae.event_code = 'IT' THEN 'ITEM_RETURN'
        
    END AS event_code,
    kom.csi_facility_id as office_id,
    kom.office_name,
    'ext_pdmanagement_article_event' AS source_table,
    '' AS delivery_status,
    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event kae
JOIN mis_db.ext_mdm_office_master kom ON kae.current_office_id = kom.office_id
select * from mis_db.ext_mdm_office_master
select * from mis_db.mv_new_article_events_tracking_event

----------------------7
INSERT INTO mis_db.tracking_event_mv
SELECT article_number, invoice_date AS event_date, 'Item Invoiced' AS event_type, office_id, kom.office_name,
       'ext_pdmanagement_unregisteredarticle_dateentry' AS source_table, '' AS delivery_status, 
       5 AS sort_order
FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
JOIN mis_db.ext_mdm_office_master kom USING (office_id)
WHERE invoice_date IS NOT NULL --AND invoice_date >= TIMESTAMP '2025-06-19 00:00:00';
CREATE MATERIALIZED VIEW mis_db.mv_new_pdmanagement_unregisteredarticle_dateentry_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_tracking_event_mv
AS
insert into mis_db.new_customer_tracking_event_mv
SELECT article_number, invoice_date AS event_date, 'Item Invoiced' AS event_type, 'ITEM_INVOICE' AS event_code, office_id, kom.office_name,
       'ext_pdmanagement_unregisteredarticle_dateentry' AS source_table, '' AS delivery_status, 
       5 AS sort_order
FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
JOIN mis_db.ext_mdm_office_master kom USING (office_id)
WHERE invoice_date IS NOT NULL 
select * from mis_db.ext_pdmanagement_unregisteredarticle_dateentry
select distinct(status) from mis_db.ext_pdmanagement_unregisteredarticle_dateentry
-------------------------------------8
INSERT INTO mis_db.tracking_event_mv
SELECT article_number, remarks_date AS event_date, 'Item Delivery' AS event_type, 'ITEM_DELIVERY' AS event_code,office_id, kom.office_name,
       'ext_pdmanagement_unregisteredarticle_dateentry' AS source_table, '' AS delivery_status, 
       5 AS sort_order
FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
JOIN mis_db.ext_mdm_office_master kom USING (office_id)
WHERE status = 2 --AND remarks_date >= TIMESTAMP '2025-06-19 00:00:00';

CREATE MATERIALIZED VIEW mis_db.mv_new_pdmanagement_unregisteredarticle_delivered_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_tracking_event_mv
AS
insert into 
SELECT article_number, remarks_date AS event_date, 'Item Delivered' AS event_type, office_id, kom.office_name,
       'ext_pdmanagement_unregisteredarticle_dateentry' AS source_table, '' AS delivery_status, 
       5 AS sort_order
FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
JOIN mis_db.ext_mdm_office_master kom USING (office_id)
WHERE status = 2 --AND remarks_date >= TIMESTAMP '2025-06-19 00:00:00';


------------------9
INSERT INTO mis_db.tracking_event_mv
SELECT karr.article_number, karr.transaction_date AS event_date, 'Item Recalled' AS event_type, kom.office_id, kom.office_name,
       'ext_pdmanagement_article_recall_return' AS source_table, '' AS delivery_statys, 
       6 AS sort_order
FROM mis_db.ext_pdmanagement_article_recall_return karr
JOIN (
    SELECT article_number, md_office_id_bkg FROM mis_db.ext_mailbkg_mailbooking_dom
    UNION ALL
    SELECT article_number, md_office_id_bkg FROM mis_db.ext_mailbkg_mailbooking_intl
) kmd ON karr.article_number = kmd.article_number
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE karr.transaction_date >= TIMESTAMP '2025-06-19 00:00:00';

INSERT INTO mis_db.tracking_event_mv
SELECT
    karr.article_number,
    karr.transaction_date AS event_date,
    'Item Recalled' AS event_type,
    kom.office_id,
    kom.office_name,
    'ext_pdmanagement_article_recall_return' AS source_table,
    '' AS delivery_statys,
    6 AS sort_order
FROM mis_db.ext_pdmanagement_article_recall_return AS karr
JOIN mis_db.ext_mailbkg_mailbooking_dom AS kmd
    ON karr.article_number = kmd.article_number
JOIN mis_db.ext_mdm_office_master AS kom
    ON kmd.md_office_id_bkg = kom.office_id
--WHERE karr.transaction_date >= TIMESTAMP '2025-06-19 00:00:00'

UNION ALL

SELECT
    karr.article_number,
    karr.transaction_date AS event_date,
    'Item Recalled' AS event_type,
    kom.office_id,
    kom.office_name,
    'ext_pdmanagement_article_recall_return' AS source_table,
    '' AS delivery_statys,
    6 AS sort_order
FROM mis_db.ext_pdmanagement_article_recall_return AS karr
JOIN mis_db.ext_mailbkg_mailbooking_intl AS kmd
    ON karr.article_number = kmd.article_number
JOIN mis_db.ext_mdm_office_master AS kom
    ON kmd.md_office_id_bkg = kom.office_id
--WHERE karr.transaction_date >= TIMESTAMP '2025-06-19 00:00:00';
    drop table mis_db.mv_new_pdmanagement_article_recall_tracking_event
CREATE MATERIALIZED VIEW IF NOT EXISTS mis_db.mv_new_pdmanagement_article_recall_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_tracking_event_mv
AS
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
INNER JOIN mis_db.ext_mailbkg_mailbooking_dom AS kmd
    ON karr.article_number = kmd.article_number
INNER JOIN mis_db.ext_mdm_office_master AS kom
    ON kmd.md_office_id_bkg = kom.office_id

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
INNER JOIN mis_db.ext_mailbkg_mailbooking_intl AS kmd
    ON karr.article_number = kmd.article_number
INNER JOIN mis_db.ext_mdm_office_master AS kom
    ON kmd.md_office_id_bkg = kom.office_id;

------------------10
INSERT INTO mis_db.tracking_event_mv
SELECT article_number, now(), 'Delivery Status' AS event_type, -1 AS office_id, 'SYSTEM' AS office_name,
'ext_pdmanagement_article_transaction' AS source_table,
       CASE WHEN action_code = 1 THEN 'delivered' ELSE 'not delivered' END AS delivery_status, 7 AS sort_order
FROM mis_db.ext_pdmanagement_article_transaction
WHERE article_number IS NOT NULL;

drop table mis_db.mv_pdmanagement_article_transaction_tracking_event ON CLUSTER cluster_1S_2R --dropped

CREATE MATERIALIZED VIEW mis_db.mv_pdmanagement_article_transaction_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
SELECT article_number, now() as event_date, '' AS event_type, -1 AS office_id, '' AS office_name,
'ext_pdmanagement_article_transaction' AS source_table,
       CASE WHEN action_code = 1 THEN 'delivered' ELSE epat.remarks AS delivery_status, 7 AS sort_order
FROM mis_db.ext_pdmanagement_article_transaction epat
WHERE article_number in (select article_number from mis_db.customer_xml_customer_mv where bulk_customer_id = '1000002954')   IS NOT NULL;
select * from mis_db.ext_pdmanagement_article_transaction

select * from mis_db.new_customer_xml_facility_customer_mv ncxfcm 
drop table mis_db.mv_new_pdmanagement_article_transaction_tracking_event
CREATE MATERIALIZED VIEW mis_db.mv_new_pdmanagement_article_transaction_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_tracking_event_mv
AS
--insert into mis_db.new_customer_tracking_event_mv



------------------------

-- mis_db.mv_new_bag_events_tracking_event source
DROP MATERIALIZED VIEW IF EXISTS mis_db.mv_new_pdmanagement_article_transaction_tracking_event ON CLUSTER cluster_1S_2R;
DROP TABLE IF EXISTS mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R;
DROP MATERIALIZED VIEW IF EXISTS mis_db.mv_new_bag_events_tracking_event ON CLUSTER cluster_1S_2R;
drop table mis_db.mv_new_bag_events_tracking_event 

CREATE MATERIALIZED VIEW mis_db.mv_new_bag_events_tracking_event TO mis_db.new_customer_tracking_event_mv
(

    `article_number` String,

    `event_date` DateTime64(6),

    `event_type` String,

    `event_code` String,

    `office_id` Int32,

    `office_name` String,

    `source_table` String,

    `delivery_status` String,

    `sort_order` UInt8
)
AS SELECT
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
 NULL) AS event_type,

    multiIf(be.event_type = 'CL',
 'BAG_CLOSE',
 be.event_type = 'DI',
 'BAG_DISPATCH',
 be.event_type = 'RO',
 'TMO_RECEIVE',
 be.event_type IN ('OP',
 'OR'),
 'BAG_OPEN',
 NULL) AS event_code,

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


----------------------
-- mis_db.mv_new_customer_xml_facility_customer_mv source
drop table mis_db.mv_new_customer_xml_facility_customer_mv
select * from mis_db.mv_new_customer_xml_facility_customer_mv
CREATE MATERIALIZED VIEW mis_db.mv_new_customer_xml_facility_customer_mv TO mis_db.new_customer_xml_facility_customer_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` DateTime64(6),

    `booking_time` DateTime64(6),

    `booking_office_facility_id` String,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` String,

    `destination_office_name` String,

    `destination_pincode` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` Decimal(10,
 3),

    `tariff` Decimal(10,
 2),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64
)
AS 
Insert into new_customer_xml_facility_customer_mv
SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

--    kmd.md_updated_date AS booking_date,
--
--    kmd.md_updated_date AS booking_time,
     CAST(kmd.md_updated_date AS String) AS booking_date,
    CAST(kmd.md_updated_date AS String) AS booking_time,

    kom.csi_facility_id AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    kon.csi_facility_id AS destination_office_facility_id,

    kmd.destination_office_name AS destination_office_name,

    kmd.destination_pincode AS destination_pincode,

    kba2.city AS destination_city,

    'INDIA' AS destination_country,

    kba2.addressee_name AS receiver_name,

    kmd.bkg_ref_id AS invoice_no,

    '' AS line_item,

    kmd.charged_weight AS weight_value,

    kcd.total_amount AS tariff,

    kcd.vp_cod_value AS cod_amount,

    kmd.booking_type_code AS booking_type,

    kmd.contract_id AS contract_number,

    '' AS reference,

    kmd.bulk_customer_id AS bulk_customer_id
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.destination_office_id = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);


----------------------
-- mis_db.new_customer_xml_facility_customer_mv definition

CREATE TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R
(

    `article_number` String,

    `article_type` String,

    `booking_date` String,

    `booking_time` String,

    `booking_office_facility_id` String,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` String,

    `destination_office_name` String,

    `destination_pincode` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` Decimal(10,
 3),

    `tariff` Decimal(10,
 2),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64
)
ENGINE = ReplicatedMergeTree
ORDER BY article_number
SETTINGS index_granularity = 8192;

--
-- mis_db.mv_new_bag_events_tracking_event source

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
AS 
insert into mis_db.new_customer_tracking_event_mv
SELECT
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


---
-- mis_db.mv_new_article_events_tracking_event source

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
AS 
insert into mis_db.new_customer_tracking_event_mv
SELECT
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

-- mis_db.mv_new_article_events_tracking_event source

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

------

-- mis_db.mv_new_customer_xml_facility_customer_mv source

CREATE MATERIALIZED VIEW mis_db.mv_new_customer_xml_facility_customer_mv TO mis_db.new_customer_xml_facility_customer_mv
(

    `article_number` String,

    `article_type` String,

    `booking_date` DateTime64(6),

    `booking_time` DateTime64(6),

    `booking_office_facility_id` String,

    `booking_office_name` String,

    `booking_pin` Int32,

    `sender_address_city` String,

    `destination_office_facility_id` String,

    `destination_office_name` String,

    `destination_pincode` Int32,

    `destination_city` String,

    `destination_country` String,

    `receiver_name` String,

    `invoice_no` String,

    `line_item` String,

    `weight_value` Decimal(10,
 3),

    `tariff` Decimal(10,
 2),

    `cod_amount` Decimal(10,
 2),

    `booking_type` String,

    `contract_number` Int32,

    `reference` String,

    `bulk_customer_id` Int64
)
AS 
insert into mis_db.new_customer_xml_facility_customer_mv
SELECT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

    CAST(kmd.md_updated_date,
 'String') AS booking_date,

    CAST(kmd.md_updated_date,
 'String') AS booking_time,

    kom.csi_facility_id AS booking_office_facility_id,

    kom.office_name AS booking_office_name,

    kmd.origin_pincode AS booking_pin,

    kba1.city AS sender_address_city,

    kon.csi_facility_id AS destination_office_facility_id,

    kmd.destination_office_name AS destination_office_name,

    kmd.destination_pincode AS destination_pincode,

    kba2.city AS destination_city,

    'INDIA' AS destination_country,

    kba2.addressee_name AS receiver_name,

    kmd.bkg_ref_id AS invoice_no,

    '' AS line_item,

    kmd.charged_weight AS weight_value,

    kcd.total_amount AS tariff,

    kcd.vp_cod_value AS cod_amount,

    kmd.booking_type_code AS booking_type,

    kmd.contract_id AS contract_number,

    '' AS reference,

    kmd.bulk_customer_id AS bulk_customer_id
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.destination_office_id = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);

