insert into mis_db.tracking_event_mv 
SELECT
    cai.bag_item AS article_number,
    be.event_date AS event_date,
   

    multiIf(
        be.status = 'CL', 'Bag Close',
        be.status = 'DI', 'Bag Dispatched',
        be.status = 'RE', 'Bag Received',
        be.status = 'OP', 'Bag Opened',
        be.status = 'RD', 'Bag Received at Destination',
        be.status = 'DM', 'Bag Damaged',
        be.status = 'MR', 'Bag Misrouted',
        be.status = 'TR', 'Bag Transfer',
        be.status = 'DC', 'Bag Dispatch Cancel',

        -- New statuses from image
        be.status = 'RO', 'Re Opened',
        be.status = 'LA', 'Loaded into Another',
        be.status = 'NR', 'Not Received',
        be.status = 'BC', 'Barcode Change',
        be.status = 'LO', 'Lost',
        be.status = 'BP', 'Bag Priority Set',
        be.status = 'DL', 'Delay',
        be.status = 'NB', 'New Barcode',
        be.status = 'IA', 'Insured Approved',
        be.status = 'IH', 'Insured On Hold',
        be.status = 'DH', 'Damaged On Hold',
        be.status = 'DE', 'Delivered',
        be.status = 'OD', 'Out for Delivery',
        be.status = 'EM', 'Empty',
        be.status = 'XX', 'Station',
        be.status = 'AP', 'Approved',
        be.status = 'ND', 'Not Delivered',
        be.status = 'FO', 'Found',

        be.status IN ('OP', 'OR'), 'Item Received',
        ''
    ) AS event_type,

    be.at_facility_id AS office_id,
    be.at_office_name AS office_name,
    'csi_bag_header' AS source_table,
    '' AS delivery_status,
    9 AS sort_order

FROM mis_db.csi_bag_header AS be
LEFT JOIN mis_db.csi_article_item AS cai ON (cai.bag_id = be.bag_id)
LEFT JOIN mis_db.csi_article_event AS bcc ON (be.bag_id = bcc.article_number)



CREATE TABLE mis_db.csi_bag_events_test ON CLUSTER cluster_1S_2R
(
 cai.bag_item AS article_number,
    be.event_date AS event_date,
    be.status AS event_type1,
    
    
    `bag_item` Nullable(String),

    `event_date` DateTime64(6),

    `event_type` Nullable(String),

    `office_id` Int32,
    
)

ORDER BY (assumeNotNull(bag_id),
 assumeNotNull(bag_item))
SETTINGS index_granularity = 8192;


ENGINE = ReplicatedReplacingMergeTree(event_date)
PRIMARY KEY (article_number, bookedon)
ORDER BY (article_number, bookedon)
SETTINGS index_granularity = 8192;

select count(*) from bag_open_content where set_date >= '2025-08-04'--4943337
select count(*) from bag_close_content where set_date >= '2025-08-04'--1469014
select count(*) from bag_event where transaction_date >='04-08-2025'--46189411

WITH articles_received AS (
  SELECT 
    DATE_TRUNC('hour', be.transaction_date) AS hour_slot,
    COUNT(boc.article_number) AS total_articles_received
  FROM bag_event be
  JOIN bag_open_content boc ON be.bag_number = boc.bag_number
  WHERE be.set_date = '2025-08-04'
    AND be.event_type IN ('OR', 'OP')
  GROUP BY hour_slot
),
articles_closed AS (
  SELECT 
    DATE_TRUNC('hour', be.transaction_date) AS hour_slot,
    COUNT(bcc.article_number) AS total_articles_closed
  FROM bag_event be
  JOIN bag_close_content bcc ON be.bag_number = bcc.bag_number
  WHERE be.set_date = '2025-08-04'
    AND be.event_type = 'CL'
  GROUP BY hour_slot
),
bags_summary AS (
  SELECT 
    DATE_TRUNC('hour', transaction_date) AS hour_slot,
    COUNT(DISTINCT CASE WHEN event_type IN ('RO', 'RF') THEN bag_number END) AS total_bags_received,
    COUNT(DISTINCT CASE WHEN event_type IN ('OR', 'OP') THEN bag_number END) AS total_bags_opened,
    COUNT(DISTINCT CASE WHEN event_type IN ('CL', 'BL', 'DP') THEN bag_number END) AS total_bags_closed,
    COUNT(DISTINCT CASE WHEN event_type IN ('DI', 'OD') THEN bag_number END) AS total_bags_despatched
  FROM bag_event
  WHERE set_date = '2025-08-04'
    AND event_type IN ('RO', 'RF', 'OR', 'OP', 'CL', 'BL', 'DP', 'DI', 'OD')
  GROUP BY hour_slot
)

SELECT 
  COALESCE(bs.hour_slot, ar.hour_slot, ac.hour_slot) AS hour_slot,
  COALESCE(ar.total_articles_received, 0) AS total_articles_received,
  COALESCE(ac.total_articles_closed, 0) AS total_articles_closed,
  COALESCE(bs.total_bags_received, 0) AS total_bags_received,
  COALESCE(bs.total_bags_opened, 0) AS total_bags_opened,
  COALESCE(bs.total_bags_closed, 0) AS total_bags_closed,
  COALESCE(bs.total_bags_despatched, 0) AS total_bags_despatched
FROM bags_summary bs
FULL OUTER JOIN articles_received ar ON bs.hour_slot = ar.hour_slot
FULL OUTER JOIN articles_closed ac ON COALESCE(bs.hour_slot, ar.hour_slot) = ac.hour_slot
WHERE COALESCE(bs.hour_slot, ar.hour_slot, ac.hour_slot) >= '2025-08-04 19:00:00'
  AND COALESCE(bs.hour_slot, ar.hour_slot, ac.hour_slot) < '2025-08-04 21:00:00'
ORDER BY hour_slot;



SELECT
  COUNT(CASE WHEN bv.event_type IN ('OP', 'OR') THEN boc.article_number END) AS Article_Opened,
  COUNT(CASE WHEN bv.event_type IN ('CL', 'BL') THEN bcc.article_number END) AS Article_Closed
FROM bag_event bv
LEFT JOIN bag_open_content boc ON bv.bag_number = boc.bag_number
LEFT JOIN bag_close_content bcc ON bv.bag_number = bcc.bag_number
WHERE bv.transaction_date >= '2025-08-04' 
  AND bv.transaction_date < '2025-08-05'
  AND bv.event_type IN ('OP', 'OR', 'CL', 'BL');