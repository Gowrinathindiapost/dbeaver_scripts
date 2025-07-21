
CREATE TABLE mis_db.mv_booking_intl ON CLUSTER cluster_1S_2R
(
    article_number String,
     bookedat String,     
    bookedon DateTime,
    source_country String,
    destination_country String,
    delivery_location String,
    article_type String,
    tariff Decimal(10, 2)
    )
ENGINE = ReplicatedMergeTree
ORDER BY (article_number)
SETTINGS index_granularity = 8192;


CREATE TABLE mis_db.mv_booking_dom
(

    `article_number` String,

    `bookedat` String,

    `bookedon` String,

    `destination_pincode` Int32,

    `delivery_location` String,

    `article_type` String,

    `tariff` Float32
)
ENGINE = Log;

CREATE TABLE IF NOT EXISTS mis_db.mv_booking_dom ON CLUSTER cluster_1S_2R
(
    article_number String,
     bookedat String,     
    bookedon DateTime,
    destination_pincode Int32,
    delivery_location String,
    article_type String,
    tariff Decimal(10, 2)
    )
ENGINE = ReplicatedMergeTree
ORDER BY (article_number)
SETTINGS index_granularity = 8192;

drop table mis_db.mv_booking_dom ON CLUSTER cluster_1S_2R
drop table mis_db.mv_booking_intl ON CLUSTER cluster_1S_2R

drop table mis_db.mv_booking_dom_to_booking_dom_mv ON CLUSTER cluster_1S_2R
drop table mis_db.mv_booking_intl_to_booking_intl_mv ON CLUSTER cluster_1S_2R

INSERT INTO mis_db.mv_booking_intl
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

CREATE MATERIALIZED VIEW mis_db.mv_booking_intl_to_booking_intl_mv
ON CLUSTER cluster_1S_2R
TO mis_db.mv_booking_intl
AS
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
WHERE kmd._peerdb_is_deleted = 0;


CREATE MATERIALIZED VIEW mis_db.mv_booking_intl_to_booking_intl_mv
ON CLUSTER cluster_1S_2R
TO mis_db.mv_booking_intl
AS
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
WHERE kmd.status_code = 'PC' AND
kmd._peerdb_is_deleted = 0;
--WHERE kmd._peerdb_is_deleted = 0;
---only MV
CREATE MATERIALIZED VIEW IF NOT EXISTS mis_db.mv_booking_intl
ENGINE = ReplacingMergeTree()
ORDER BY article_number
AS
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
WHERE kmd._peerdb_is_deleted = 0;

---only MV2

CREATE MATERIALIZED VIEW IF NOT EXISTS mis_db.mv_booking_dom
ENGINE = ReplacingMergeTree()
ORDER BY article_number
AS
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
WHERE kmd.status_code = 'PC' AND
kmd._peerdb_is_deleted = 0;

INSERT INTO mis_db.mv_booking_dom
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
WHERE kmd.status_code = 'PC'


CREATE MATERIALIZED VIEW mis_db.mv_booking_dom_to_booking_dom_mv
ON CLUSTER cluster_1S_2R
TO mis_db.mv_booking_dom
AS
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
WHERE kmd.status_code = 'PC' AND
kmd._peerdb_is_deleted = 0;
--WHERE kmd._peerdb_is_deleted = 0;
-------1
CREATE TABLE IF NOT EXISTS mis_db.tracking_event_mv ON CLUSTER cluster_1S_2R
(
    article_number String,
    event_date DateTime,
    event_type String,
    office_id Int32,    
    office_name String,
    source_table String,
    delivery_status String,
    sort_order UInt8
)
ENGINE = ReplicatedMergeTree
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;
-----------2

INSERT INTO mis_db.tracking_event_mv
SELECT
    kmd.article_number,
    kmd.md_updated_date AS event_date,
    'Item Booked' AS event_type,
    kom.office_id AS office_id,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_dom' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_dom kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.status_code = 'PC'


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

drop table mis_db.mv_booking_dom_to_tracking_event ON CLUSTER cluster_1S_2R
CREATE MATERIALIZED VIEW mis_db.mv_booking_dom_to_tracking_event ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
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
FROM mis_db.ext_mailbkg_mailbooking_dom kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.status_code = 'PC' AND
kmd._peerdb_is_deleted = 0;


------3

INSERT INTO mis_db.tracking_event_mv
SELECT
    kmd.article_number,
    kmd.md_updated_date AS event_date,
    'Item Booked' AS event_type,
    kom.office_id AS office_id,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_intl' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_intl kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id 
WHERE kmd.status_code = 'PC'

drop table mis_db.mv_booking_intl_tracking_event ON CLUSTER cluster_1S_2R

CREATE MATERIALIZED VIEW mis_db.mv_booking_intl_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
SELECT
    kmd.article_number,
    kmd.md_updated_date AS event_date,
    'Item Booked' AS event_type,
    kom.office_id AS office_id,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_intl' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_intl kmd
INNER JOIN mis_db.ext_mdm_office_master kom
    ON kmd.md_office_id_bkg = kom.office_id
    WHERE kmd.status_code = 'PC' AND
kmd._peerdb_is_deleted = 0;
---------4

select * from tracking_event_mv tem where source_table='ext_bagmgmt_bag_event'  and article_number like '%IN'
INSERT INTO mis_db.tracking_event_mv
SELECT COALESCE(bcc.article_number, boc.article_number), be.transaction_date,
       CASE WHEN be.event_type = 'CL' THEN 'Item Bagged'
            WHEN be.event_type = 'DI' THEN 'Item Dispatched'
            WHEN be.event_type IN ('OP', 'OR') THEN 'Item Received' END,
       COALESCE(kom.office_id, kom2.office_id), COALESCE(kom.office_name, kom2.office_name),
       'ext_bagmgmt_bag_event' AS source_table, 
       '' AS delivery_status, 
       3 AS sort_order
       
FROM mis_db.ext_bagmgmt_bag_event be
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content bcc ON be.bag_number = bcc.bag_number AND be.event_type IN ('CL', 'DI')
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content boc ON be.bag_number = boc.bag_number AND be.event_type IN ('OP', 'OR')
LEFT JOIN mis_db.ext_mdm_office_master kom ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master kom2 ON be.to_office_id = kom2.office_id 
where bcc.article_number ='EZ771854298IN' 'EK386633306IN' 


CREATE MATERIALIZED VIEW mis_db.mv_bag_events_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
SELECT 
    COALESCE(bcc.article_number, boc.article_number) AS article_number,
    be.transaction_date AS event_date,
    CASE 
        WHEN be.event_type = 'CL' THEN 'Item Bagged'
        WHEN be.event_type = 'DI' THEN 'Item Dispatched'
        WHEN be.event_type IN ('OP', 'OR') THEN 'Item Received'
    END AS event_type,
    COALESCE(kom.office_id, kom2.office_id) AS office_id,
    COALESCE(kom.office_name, kom2.office_name) AS office_name,
    'ext_bagmgmt_bag_event' AS source_table,
    '' AS delivery_status,
    3 AS sort_order
FROM mis_db.ext_bagmgmt_bag_event AS be
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content AS bcc 
    ON be.bag_number = bcc.bag_number AND be.event_type IN ('CL', 'DI')
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content AS boc 
    ON be.bag_number = boc.bag_number AND be.event_type IN ('OP', 'OR')
LEFT JOIN mis_db.ext_mdm_office_master AS kom 
    ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 
    ON be.to_office_id = kom2.office_id where bcc.article_number='EZ771854298IN'


---------------5
INSERT INTO mis_db.tracking_event_mv
SELECT a.article_number, COALESCE(pm.updated_date, pm.assigned_date, pm.created_date), pm.pickup_status,
       kom.office_id, kom.office_name, 'kafka_pickup_main', '', 2
FROM mis_db.kafka_pickup_main pm
JOIN mis_db.kafka_domestic_article_detail a ON pm.pickup_request_id = a.pickup_request_id
JOIN mis_db.kafka_office_master kom ON pm.pickup_office_id = kom.office_id
WHERE COALESCE(pm.updated_date, pm.assigned_date, pm.created_date) >= TIMESTAMP '2025-06-19 00:00:00';

INSERT INTO mis_db.tracking_event_mv
SELECT
    a.article_number,
    CASE
        WHEN pm.pickup_status = 'Unassigned' THEN pm.created_date
        WHEN pm.pickup_status = 'Assigned' THEN pm.assigned_date
        ELSE pm.updated_date
    END AS event_date,
    pm.pickup_status AS event_type,
    kom.office_name AS office_name,
    'kafka_pickup_main' AS source_table,
    '' AS delivery_status,
    2 AS sort_order
FROM mis_db.kafka_pickup_main pm
JOIN mis_db.kafka_domestic_article_detail a ON pm.pickup_request_id = a.pickup_request_id
JOIN mis_db.ext_mdm_office_master kom ON pm.pickup_office_id = kom.office_id
WHERE (
    (pm.pickup_status = 'Unassigned' AND pm.created_date >= TIMESTAMP '2025-06-18 00:00:00') OR
    (pm.pickup_status = 'Assigned' AND pm.assigned_date >= TIMESTAMP '2025-06-18 00:00:00') OR
    (pm.pickup_status = 'Pickedup' AND pm.updated_date >= TIMESTAMP '2025-06-18 00:00:00')
);

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
    kae.remarks AS event_type,
    kom.office_id,
    kom.office_name,
    'ext_pdmanagement_article_event' AS source_table,
    '' AS delivery_status,
    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event kae
JOIN mis_db.ext_mdm_office_master kom ON kae.current_office_id = kom.office_id


CREATE MATERIALIZED VIEW mis_db.mv_article_events_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
SELECT
    kae.article_number,
    kae.event_date,
    kae.remarks AS event_type,
    kom.office_id,
    kom.office_name,
    'ext_pdmanagement_article_event' AS source_table,
    '' AS delivery_status,
    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event kae
JOIN mis_db.ext_mdm_office_master kom ON kae.current_office_id = kom.office_id

----------------------7
INSERT INTO mis_db.tracking_event_mv
SELECT article_number, invoice_date AS event_date, 'Item Invoiced' AS event_type, office_id, kom.office_name,
       'ext_pdmanagement_unregisteredarticle_dateentry' AS source_table, '' AS delivery_status, 
       5 AS sort_order
FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
JOIN mis_db.ext_mdm_office_master kom USING (office_id)
WHERE invoice_date IS NOT NULL --AND invoice_date >= TIMESTAMP '2025-06-19 00:00:00';
CREATE MATERIALIZED VIEW mis_db.mv_pdmanagement_unregisteredarticle_dateentry_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
SELECT article_number, invoice_date AS event_date, 'Item Invoiced' AS event_type, office_id, kom.office_name,
       'ext_pdmanagement_unregisteredarticle_dateentry' AS source_table, '' AS delivery_status, 
       5 AS sort_order
FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
JOIN mis_db.ext_mdm_office_master kom USING (office_id)
WHERE invoice_date IS NOT NULL 
-------------------------------------8
INSERT INTO mis_db.tracking_event_mv
SELECT article_number, remarks_date AS event_date, 'Item Delivered' AS event_type, office_id, kom.office_name,
       'ext_pdmanagement_unregisteredarticle_dateentry' AS source_table, '' AS delivery_status, 
       5 AS sort_order
FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
JOIN mis_db.ext_mdm_office_master kom USING (office_id)
WHERE status = 2 --AND remarks_date >= TIMESTAMP '2025-06-19 00:00:00';

CREATE MATERIALIZED VIEW mis_db.mv_pdmanagement_unregisteredarticle_delivered_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
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
CREATE MATERIALIZED VIEW IF NOT EXISTS mis_db.mv_pdmanagement_article_recall_tracking_event
ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
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
       CASE WHEN action_code = 1 THEN 'delivered' ELSE 'not delivered' END AS delivery_status, 7 AS sort_order
FROM mis_db.ext_pdmanagement_article_transaction
WHERE article_number IS NOT NULL;


select * from tracking_event_mv tem where article_number='EZ771854298IN' source_table='ext_pdmanagement_article_transaction' article_number='CK641868914IN'

SELECT *
FROM tracking_event_mv
GROUP BY article_number, event_date
HAVING count() > 1;

OPTIMIZE TABLE target_table FINAL;

SELECT * FROM tracking_event_mv FINAL;

OPTIMIZE TABLE mis_db.tracking_event_mv FINAL;


SELECT article_number, max(event_date) AS event_date
FROM mis_db.tracking_event_mv where article_number ='RK000321191IN'
GROUP BY article_number;

select * from mv_booking_dom where article_number='EK386990362IN'
select * from tracking_event_mv where article_number='EK386990362IN'

select * from mv_booking_dom where bookedon > '2025-05-15 00:00:00' and bookedon < '2025-05-15 15:53:48'
SELECT * FROM mv_booking_dom 
WHERE booked_on > '2025-05-25 15:53:48';

SELECT * 
FROM mv_booking_dom 
WHERE bookedon > '2025-05-15 00:00:00' 
  AND bookedon < '2025-05-17 00:00:00';


SELECT * 
FROM mv_booking_dom 
WHERE bookedon >= '2025-06-15 00:00:00' 
  AND bookedon <  '2025-06-16 00:00:00';


EK386990362IN

SELECT * FROM MIS_DB.EXT_MAILBKG_MAILBOOKING_DOM


select distinct(a.event_time),a.article_number,a.event_cd ,a.office_cd,iem.event_name,
--om.ooe_name,om.pincode   
from mis_db.ext_ips_ips_event a --where a.article_number ='RD220302500IN' 
JOIN mis_db.ext_ips_ips_event_master iem on iem.event_cd= a.event_cd 
--JOIN mis_db.ooe_master om on om.ooe_fcd= a.office_cd 
where a.article_number in ('RD220302500IN','CT530202963IN','CE027445290SA','CE032988665SA')

select * from 


CREATE MATERIALIZED VIEW mis_db.mv_ips_event_tracking_event_mv
ON CLUSTER cluster_1S_2R
ORDER BY article_number
--TO mis_db.tracking_event_mv
AS
SELECT article_number, event_time AS event_date, event_name AS event_type, '99999999' AS office_id, ooe_name AS office_name, --kom.office_name,
       'ext_ips_ips_event' AS source_table, '' AS delivery_status, 
       '' AS sort_order
FROM mis_db.ext_ips_ips_event
JOIN mis_db.ext_ips_ips_event_master kom USING (event_cd)
JOIN mis_db.dim_ips_ooe_master ooe using(ooe_fcd)

---

CREATE MATERIALIZED VIEW mis_db.mv_ips_event_tracking_event
ON CLUSTER cluster_1S_2R
ORDER BY article_number
--TO mis_db.tracking_event_mv
AS
SELECT article_number, event_time AS event_date, event_name AS event_type, '99999999' AS office_id, --kom.office_name,
       'ext_ips_ips_event' AS source_table, '' AS delivery_status, 
       '' AS sort_order
FROM mis_db.ext_ips_ips_event
JOIN mis_db.ext_ips_ips_event_master kom USING (event_cd)
WHERE kmd._peerdb_is_deleted = 0;
--WHERE status = 2 --AND remarks_date >= TIMESTAMP '2025-06-19 00:00:00';


CREATE MATERIALIZED VIEW mis_db.mv_ips_event_tracking_event_mv
ON CLUSTER cluster_1S_2R
ORDER BY article_number
--TO mis_db.tracking_event_mv -- This line is commented out, meaning it's a regular MV without a destination table.
AS
SELECT
    -- Use ifNull to replace NULL or empty article_number with 'UNKNOWN_ARTICLE'
t1.article_number,
    t1.event_time AS event_date,
    t2.event_name AS event_type,
    '99999999' AS office_id, -- Now taking office_id from ext_ips_ips_event_master (t2)
    t3.ooe_name AS office_name,
    'ext_ips_ips_event' AS source_table,
    '' AS delivery_status,
    '' AS sort_order
FROM mis_db.ext_ips_ips_event AS t1
JOIN mis_db.ext_ips_ips_event_master AS t2 ON t1.event_cd = t2.event_cd
-- Corrected join: Use t2.office_id to link to t3.ooe_fcd
JOIN mis_db.dim_ips_ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd;

drop table mis_db.mv_ips_event_ooe_master_tracking_event ON CLUSTER cluster_1S_2R
CREATE MATERIALIZED VIEW mis_db.mv_ips_event_ooe_master_tracking_event
ON CLUSTER cluster_1S_2R
--ORDER BY article_number
TO mis_db.tracking_event_mv -- This line is commented out, meaning it's a regular MV without a destination table.
AS
SELECT
    -- Use ifNull to replace NULL or empty article_number with 'UNKNOWN_ARTICLE'
t1.article_number,
    t1.event_time AS event_date,
    t2.event_name AS event_type,
    '99999999' AS office_id, -- Now taking office_id from ext_ips_ips_event_master (t2)
    t3.ooe_name AS office_name,
    'ext_ips_ips_event' AS source_table,
    '' AS delivery_status,
    8 AS sort_order
FROM mis_db.ext_ips_ips_event AS t1
JOIN mis_db.ext_ips_ips_event_master AS t2 ON t1.event_cd = t2.event_cd
-- Corrected join: Use t2.office_id to link to t3.ooe_fcd
JOIN mis_db.dim_ips_ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd;
 
select * from mis_db.tracking_event_mv tem where tem.article_number ='RK700092125IN''CA072771595IN'