SELECT
    formatDateTime(be.transaction_date, '%d%m%Y') AS EventDate,
    formatDateTime(be.transaction_date, '%H%M%S') AS EventTime,
    CASE 
        WHEN be.event_type IN ('CL', 'DI') THEN kom.office_name
        WHEN be.event_type IN ('RO', 'OP', 'OR') THEN kom2.office_name
        ELSE NULL
    END AS EventOfficeName,
    CASE 
        WHEN be.event_type IN ('CL', 'DI') THEN be.from_office_id
        WHEN be.event_type IN ('RO', 'OP', 'OR') THEN be.to_office_id
        ELSE NULL
    END AS EventOfficeFaciltyID,
    CASE 
        WHEN be.event_type = 'CL' THEN 'BAG_CLOSE'
        WHEN be.event_type IN ('OP', 'OR') THEN 'BAG_OPEN'
        WHEN be.event_type = 'DI' THEN 'BAG_DISPATCH'
        WHEN be.event_type = 'RO' THEN 'TMO_RECEIVE'
    END AS EventCode,
    CASE 
        WHEN be.event_type = 'CL' THEN 'Bag Close'
        WHEN be.event_type IN ('OP', 'OR') THEN 'Bag Open'
        WHEN be.event_type = 'DI' THEN 'Bag Dispatch'
        WHEN be.event_type = 'RO' THEN 'Item Received'
    END AS EventDescription
FROM mis_db.ext_bagmgmt_bag_event AS be
INNER JOIN mis_db.ext_mdm_office_master AS kom
    ON be.from_office_id = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kom2
    ON be.to_office_id = kom2.office_id
INNER JOIN (
    SELECT bag_number FROM mis_db.ext_bagmgmt_bag_close_content
    WHERE article_number = 'CBK3015530484'
    UNION DISTINCT
    SELECT bag_number FROM mis_db.ext_bagmgmt_bag_open_content
    WHERE article_number = 'CBK3015530484'
) AS BagNumbers
    ON be.bag_number = BagNumbers.bag_number
ORDER BY be.transaction_date DESC
LIMIT 1;

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
Insert into mis_db.new_customer_tracking_event_mv
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

    coalesce(kom.csi_facility_id,
 kom2.csi_facility_id) AS office_id,
 
-- kom.csi_facility_id as office_id,

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

select * from mis_db.ext_mdm_office_master

