
-- Create target table for tracking events
CREATE TABLE IF NOT EXISTS mis_db.tracking_event_mv ON CLUSTER cluster_1S_2R
(
    article_number String,
    event_date DateTime,
    event_type String,
    office_name String,
    source_table String,
    delivery_status String,
    sort_order UInt8
)
ENGINE = ReplicatedMergeTree
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;

--ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}',
-- '{replica}',
-- mailbooking_id)
--PRIMARY KEY (mailbooking_id,
-- book_table)
--ORDER BY (mailbooking_id,
-- book_table)
--SETTINGS index_granularity = 8192;
-- Drop and create materialized view to auto-populate the tracking_event_mv table (optional if streaming is done manually)
DROP TABLE IF EXISTS mis_db.mv_tracking_event_mv;

CREATE MATERIALIZED VIEW mis_db.mv_tracking_event_mv
TO mis_db.tracking_event_mv
AS
SELECT *
FROM mis_db.tracking_event_mv;

CREATE MATERIALIZED VIEW mis_db.mv_raw_bagmgmt_bag_content_open ON CLUSTER cluster_1S_2R TO mis_db.raw_bagmgmt_bag_content
AS
SELECT bag_number, article_number, article_type, art_status, insured_flag, bag_open_content_id bag_content_id, booking_reference_id, 1 bag_action
FROM mis_db.ext_bagmgmt_bag_open_content;


-- Backfill Booking DOM
INSERT INTO mis_db.tracking_event_mv
SELECT
    kmd.article_number,
    kmd.md_created_date AS event_date,
    'Item Booked' AS event_type,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_dom' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_dom kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.md_created_date >= TIMESTAMP '2025-06-18 00:00:00';

-- Backfill Booking INTL
INSERT INTO mis_db.tracking_event_mv
SELECT
    kmd.article_number,
    kmd.md_updated_date AS event_date,
    'Item Booked' AS event_type,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_intl' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_intl kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.md_updated_date >= TIMESTAMP '2025-06-18 00:00:00';

-- Backfill Pickup Events
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

-- Backfill Bag Events
INSERT INTO mis_db.tracking_event_mv
SELECT
    COALESCE(boc.article_number, bo.article_number) AS article_number,
    be.transaction_date AS event_date,
    CASE be.event_type
        WHEN 'CL' THEN 'Item Bagged'
        WHEN 'DI' THEN 'Item Dispatched'
        WHEN 'OP' THEN 'Item Received'
        WHEN 'OR' THEN 'Item Received'
    END AS event_type,
    CASE
        WHEN be.event_type IN ('CL', 'DI') THEN kom.office_name
        WHEN be.event_type IN ('OP', 'OR') THEN kom2.office_name
        ELSE ''
    END AS office_name,
    'ext_bagmgmt_bag_event' AS source_table,
    '' AS delivery_status,
    3 AS sort_order
FROM mis_db.ext_bagmgmt_bag_event be
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content boc ON be.bag_number = boc.bag_number AND be.event_type IN ('CL', 'DI')
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content bo ON be.bag_number = bo.bag_number AND be.event_type IN ('OP', 'OR')
LEFT JOIN mis_db.ext_mdm_office_master kom ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master kom2 ON be.to_office_id = kom2.office_id
WHERE (boc.article_number IS NOT NULL OR bo.article_number IS NOT NULL)
AND be.transaction_date >= TIMESTAMP '2025-06-18 00:00:00';

-- Backfill Delivery Events
INSERT INTO mis_db.tracking_event_mv
SELECT
    kae.article_number,
    kae.event_date,
    kae.remarks AS event_type,
    kom.office_name,
    'ext_pdmanagement_article_event' AS source_table,
    '' AS delivery_status,
    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event kae
JOIN mis_db.ext_mdm_office_master kom ON kae.current_office_id = kom.office_id
WHERE kae.event_date >= TIMESTAMP '2025-06-18 00:00:00';

-- Backfill Unregistered Articles
INSERT INTO mis_db.tracking_event_mv
SELECT
    article_number,
    event_date,
    event_type,
    kom.office_name,
    'ext_pdmanagement_unregisteredarticle_dateentry' AS source_table,
    '' AS delivery_status,
    5 AS sort_order
FROM (
    SELECT
        article_number,
        invoice_date AS event_date,
        'Item Invoiced' AS event_type,
        office_id
    FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
    WHERE invoice_date >= TIMESTAMP '2025-06-18 00:00:00'

    UNION ALL

    SELECT
        article_number,
        remarks_date AS event_date,
        'Item Delivered' AS event_type,
        office_id
    FROM mis_db.ext_pdmanagement_unregisteredarticle_dateentry
    WHERE status = 2 AND remarks_date >= TIMESTAMP '2025-06-18 00:00:00'
) a
JOIN mis_db.ext_mdm_office_master kom ON kom.office_id = a.office_id;

-- Backfill Recall Events
INSERT INTO mis_db.tracking_event_mv
SELECT
    karr.article_number,
    karr.transaction_date AS event_date,
    'Item Recalled' AS event_type,
    kom.office_name AS office_name,
    'ext_pdmanagement_article_recall_return' AS source_table,
    '' AS delivery_status,
    6 AS sort_order
FROM mis_db.ext_pdmanagement_article_recall_return karr
JOIN (
    SELECT article_number, md_office_id_bkg 
    FROM mis_db.ext_mailbkg_mailbooking_dom
    UNION ALL
    SELECT article_number, md_office_id_bkg 
    FROM mis_db.ext_mailbkg_mailbooking_intl
) kmd ON karr.article_number = kmd.article_number
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE karr.transaction_date >= TIMESTAMP '2025-06-18 00:00:00';

-- Backfill Delivery Status
INSERT INTO mis_db.tracking_event_mv
SELECT
    article_number,
    now() AS event_date,
    '' AS event_type,
    '' AS office_name,
    'ext_pdmanagement_article_transaction' AS source_table,
    CASE WHEN action_code = 1 THEN 'delivered' ELSE 'not delivered' END AS delivery_status,
    7 AS sort_order
FROM mis_db.ext_pdmanagement_article_transaction
WHERE updated_at >= TIMESTAMP '2025-06-18 00:00:00';


CREATE MATERIALIZED VIEW mis_db.mv_raw_bagmgmt_bag_content_open ON CLUSTER cluster_1S_2R
AS
SELECT bag_number, article_number, article_type, art_status, insured_flag, bag_open_content_id bag_content_id, booking_reference_id, 1 bag_action
FROM mis_db.ext_bagmgmt_bag_open_content;


CREATE MATERIALIZED VIEW mis_db.mv_booking_dom_to_tracking_event_mv ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
SELECT
    kmd.article_number,
    kmd.md_created_date AS event_date,
    'Item Booked' AS event_type,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_dom' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_dom kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id;

DROP TABLE mis_db.mv_booking_dom_to_tracking_event_mv;
DROP TABLE mis_db.tracking_event_mv;

