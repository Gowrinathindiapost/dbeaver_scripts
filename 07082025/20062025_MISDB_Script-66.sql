

DESCRIBE TABLE mis_db.ext_mdm_office_master;

CREATE MATERIALIZED VIEW mis_db.mv_booking_dom_to_tracking_event_mv ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
SELECT
    kmd.article_number,
    kmd.md_created_date AS event_date,
    kom.office_id AS office_id,
    'Item Booked' AS event_type,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_dom' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_dom kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id;

DROP TABLE mis_db.mv_booking_dom_to_tracking_event_mv;
DROP TABLE mis_db.tracking_event_mv;
-------------------------------------------------------------
CREATE MATERIALIZED VIEW mis_db.mv_booking_dom_to_tracking_event_mv ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv 
AS
SELECT
    kmd.article_number AS article_number,                      -- Explicitly alias
    kmd.md_created_date AS event_date,                         -- Already aliased
    CAST(kom.office_id AS Int32) AS office_id,                 -- Explicitly cast and alias to resolve matching issue
    'Item Booked' AS event_type,                               -- Already aliased
    kom.office_name AS office_name,                            -- Already aliased
    'ext_mailbkg_mailbooking_dom' AS source_table,             -- Already aliased
    '' AS delivery_status,                                     -- Already aliased
    1 AS sort_order                                            -- Already aliased
FROM mis_db.ext_mailbkg_mailbooking_dom kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id;
----------------------------------------------------------
CREATE TABLE IF NOT EXISTS mis_db.tracking_event_mv ON CLUSTER cluster_1S_2R
(
    article_number String,
    event_date DateTime,
    office_id Int32,
    event_type String,
    office_name String,
    source_table String,
    delivery_status String,
    sort_order UInt8
)
ENGINE = ReplicatedMergeTree
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;

-- Create target table with office_id
CREATE TABLE IF NOT EXISTS trackandtrace.tracking_event_mv
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
ENGINE = MergeTree
ORDER BY (article_number, event_date);

-- Drop and create materialized view
DROP TABLE IF EXISTS trackandtrace.mv_tracking_event_mv;

CREATE MATERIALIZED VIEW trackandtrace.mv_tracking_event_mv
TO trackandtrace.tracking_event_mv
AS
SELECT *
FROM trackandtrace.tracking_event_mv;
--------------



-- Backfill Booking DOM
INSERT INTO mis_db.tracking_event_mv
SELECT
    kmd.article_number,
    kmd.md_created_date AS event_date,
    kom.office_id,
    'Item Booked' AS event_type,
    kom.office_name,
    'ext_mailbkg_mailbooking_dom' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_dom kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.md_created_date >= TIMESTAMP '2025-06-19 00:00:00';

-- Backfill Article Events
INSERT INTO mis_db.tracking_event_mv
SELECT
    kae.article_number,
    kae.event_date,
     kom.office_id,
    kae.remarks AS event_type,
    kom.office_name,
    'ext_pdmanagement_article_event' AS source_table,
    '' AS delivery_status,
    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event kae
JOIN mis_db.ext_mdm_office_master kom ON kae.current_office_id = kom.office_id
WHERE kae.event_date >= TIMESTAMP '2025-06-18 00:00:00';


CREATE MATERIALIZED VIEW mis_db.mv_article_event_to_tracking_event_mv ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
SELECT
    kae.article_number AS article_number,
    kae.event_date AS event_date,
    CAST(kom.office_id AS Int32) AS office_id, -- Explicitly cast to Int32 and alias
    kae.remarks AS event_type,
    kom.office_name AS office_name,
    'ext_pdmanagement_article_event' AS source_table,
    '' AS delivery_status,
    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event kae
JOIN mis_db.ext_mdm_office_master kom ON kae.current_office_id = kom.office_id
WHERE kae.event_date >= TIMESTAMP '2025-06-18 00:00:00';


---------------------
---------------------
-- Booking INTL
INSERT INTO trackandtrace.tracking_event_mv
SELECT kmd.article_number, kmd.md_created_date, 'Item Booked', kom.office_id, kom.office_name,
       'kafka_mailbooking_intl', '', 1
FROM trackandtrace.kafka_mailbooking_intl kmd
JOIN trackandtrace.kafka_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.md_created_date >= TIMESTAMP '2025-06-19 00:00:00';

INSERT INTO mis_db.tracking_event_mv
SELECT
    kmd.article_number,
    kmd.md_updated_date AS event_date,
    kom.office_id AS office_id,
    'Item Booked' AS event_type,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_intl' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_intl kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.md_updated_date >= TIMESTAMP '2025-06-18 00:00:00';

CREATE MATERIALIZED VIEW mis_db.mv_article_event_to_tracking_event_mv ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
SELECT
    kae.article_number AS article_number,
    kae.event_date AS event_date,
    CAST(kom.office_id AS Int32) AS office_id, -- Explicitly cast to Int32 and alias
    kae.remarks AS event_type,
    kom.office_name AS office_name,
    'ext_pdmanagement_article_event' AS source_table,
    '' AS delivery_status,
    4 AS sort_order
FROM mis_db.ext_pdmanagement_article_event kae
JOIN mis_db.ext_mdm_office_master kom ON kae.current_office_id = kom.office_id
WHERE kae.event_date >= TIMESTAMP '2025-06-18 00:00:00';
CREATE MATERIALIZED VIEW mis_db.booked_intl_to_tracking_event_mv ON CLUSTER cluster_1S_2R
TO mis_db.tracking_event_mv
AS
SELECT
    kmd.article_number,
    kmd.md_updated_date AS event_date,
    kom.office_id AS office_id,
    'Item Booked' AS event_type,
    kom.office_name AS office_name,
    'ext_mailbkg_mailbooking_intl' AS source_table,
    '' AS delivery_status,
    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_intl kmd
JOIN mis_db.ext_mdm_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.md_updated_date >= TIMESTAMP '2025-06-18 00:00:00'
WITH DATA;

-- Pickup
INSERT INTO trackandtrace.tracking_event_mv
SELECT a.article_number, COALESCE(pm.updated_date, pm.assigned_date, pm.created_date), pm.pickup_status,
       kom.office_id, kom.office_name, 'kafka_pickup_main', '', 2
FROM trackandtrace.kafka_pickup_main pm
JOIN trackandtrace.kafka_domestic_article_detail a ON pm.pickup_request_id = a.pickup_request_id
JOIN trackandtrace.kafka_office_master kom ON pm.pickup_office_id = kom.office_id
WHERE COALESCE(pm.updated_date, pm.assigned_date, pm.created_date) >= TIMESTAMP '2025-06-19 00:00:00';

-- Bag events
INSERT INTO trackandtrace.tracking_event_mv
SELECT COALESCE(bcc.article_number, boc.article_number), be.transaction_date,
       CASE WHEN be.event_type = 'CL' THEN 'Item Bagged'
            WHEN be.event_type = 'DI' THEN 'Item Dispatched'
            WHEN be.event_type IN ('OP', 'OR') THEN 'Item Received' END,
       COALESCE(kom.office_id, kom2.office_id), COALESCE(kom.office_name, kom2.office_name),
       'kafka_bag_event', '', 3
FROM trackandtrace.kafka_bag_event be
LEFT JOIN trackandtrace.kafka_bag_close_content bcc ON be.bag_number = bcc.bag_number AND be.event_type IN ('CL', 'DI')
LEFT JOIN trackandtrace.kafka_bag_open_content boc ON be.bag_number = boc.bag_number AND be.event_type IN ('OP', 'OR')
LEFT JOIN trackandtrace.kafka_office_master kom ON be.from_office_id = kom.office_id
LEFT JOIN trackandtrace.kafka_office_master kom2 ON be.to_office_id = kom.office_id
WHERE be.transaction_date >= TIMESTAMP '2025-06-19 00:00:00';

-- Delivery Events
INSERT INTO trackandtrace.tracking_event_mv
SELECT kae.article_number, kae.event_date, kae.remarks, kom.office_id, kom.office_name,
       'kafka_article_event', '', 4
FROM trackandtrace.kafka_article_event kae
JOIN trackandtrace.kafka_office_master kom ON kae.current_office_id = kom.office_id
WHERE kae.event_date >= TIMESTAMP '2025-06-19 00:00:00';

-- Unreg invoiced
INSERT INTO trackandtrace.tracking_event_mv
SELECT article_number, invoice_date, 'Item Invoiced', office_id, kom.office_name,
       'kafka_unregisteredarticle_dateentry', '', 5
FROM trackandtrace.kafka_unregisteredarticle_dateentry
JOIN trackandtrace.kafka_office_master kom USING (office_id)
WHERE invoice_date IS NOT NULL AND invoice_date >= TIMESTAMP '2025-06-19 00:00:00';

-- Unreg delivered
INSERT INTO trackandtrace.tracking_event_mv
SELECT article_number, remarks_date, 'Item Delivered', office_id, kom.office_name,
       'kafka_unregisteredarticle_dateentry', '', 5
FROM trackandtrace.kafka_unregisteredarticle_dateentry
JOIN trackandtrace.kafka_office_master kom USING (office_id)
WHERE status = 2 AND remarks_date >= TIMESTAMP '2025-06-19 00:00:00';

-- Recall
INSERT INTO trackandtrace.tracking_event_mv
SELECT karr.article_number, karr.transaction_date, 'Item Recalled', kom.office_id, kom.office_name,
       'kafka_article_recall_return', '', 6
FROM trackandtrace.kafka_article_recall_return karr
JOIN (
    SELECT article_number, md_office_id_bkg FROM trackandtrace.kafka_mailbooking_dom
    UNION ALL
    SELECT article_number, md_office_id_bkg FROM trackandtrace.kafka_mailbooking_intl
) kmd ON karr.article_number = kmd.article_number
JOIN trackandtrace.kafka_office_master kom ON kmd.md_office_id_bkg = kom.office_id
WHERE karr.transaction_date >= TIMESTAMP '2025-06-19 00:00:00';

-- Delivery status
INSERT INTO trackandtrace.tracking_event_mv
SELECT article_number, now(), 'Delivery Status', -1, 'SYSTEM', 'kafka_article_transaction',
       CASE WHEN action_code = 1 THEN 'delivered' ELSE 'not delivered' END, 7
FROM trackandtrace.kafka_article_transaction
WHERE article_number IS NOT NULL;

