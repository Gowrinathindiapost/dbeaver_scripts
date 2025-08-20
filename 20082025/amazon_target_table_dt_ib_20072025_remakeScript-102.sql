
drop table mis_db.amazon_target_table_dt_ib_mv ON CLUSTER cluster_1S_2R 
CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_ib_mv TO mis_db.amazon_target_table_dt_ib
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
    `weight_value` Decimal(10, 3),
    `tariff` Decimal(10, 2),
    `cod_amount` Decimal(10, 2),
    `booking_type` String,
    `contract_number` Int32,
    `reference` String,
    `bulk_customer_id` Int64,
    `event_date` DateTime64(6),
    `event_time` DateTime64(6),
    `event_code` String,
    `event_office_facilty_id` String,
    `office_name` String,
    `event_description` String,
    `non_delivery_reason` String

)
AS
insert into mis_db.amazon_target_table_dt_ib
SELECT DISTINCT
    cxcm.article_number AS article_number,
    cxcm.article_type AS article_type,
    cxcm.booking_date AS booking_date,
    cxcm.booking_time AS booking_time,
    cxcm.booking_office_facility_id AS booking_office_facility_id,
    cxcm.booking_office_name AS booking_office_name,
    cxcm.booking_pin AS booking_pin,
    cxcm.sender_address_city AS sender_address_city,
    cxcm.destination_office_facility_id AS destination_office_facility_id,
    cxcm.destination_office_name AS destination_office_name,
    cxcm.destination_pincode AS destination_pincode,
    cxcm.destination_city AS destination_city,
    cxcm.destination_country AS destination_country,
    cxcm.receiver_name AS receiver_name,
    cxcm.invoice_no AS invoice_no,
    cxcm.line_item AS line_item,
    cxcm.weight_value AS weight_value,
    cxcm.tariff AS tariff,
    cxcm.cod_amount AS cod_amount,
    cxcm.booking_type AS booking_type,
    cxcm.contract_number AS contract_number,
    cxcm.reference AS reference,
    cxcm.bulk_customer_id AS bulk_customer_id,

    te.event_date AS event_date,
    -- Fix duplication by using a different source or expression
    toDateTime64(te.event_date, 6) + INTERVAL 0 SECOND AS event_time,

    te.event_code AS event_code,
    te.office_id AS event_office_facilty_id,
    te.office_name AS office_name,
    te.event_type AS event_description,
    te.delivery_status AS non_delivery_reason

FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
LEFT JOIN
(
    SELECT
        article_number,
        event_date,
        event_code,
        office_id,
        office_name,
        event_type,
        delivery_status
    FROM mis_db.new_customer_tracking_event_new_mv
    WHERE event_code = 'ITEM_BOOK'
) AS te
    ON cxcm.article_number = te.article_number

LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id

WHERE cxcm.bulk_customer_id != 0
  AND te.article_number IS NOT NULL;

--old mv query is below
-- `mis_db`.`amazon_target_table_dt_ib_mv` source

CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_ib_mv TO mis_db.amazon_target_table_dt_ib
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

    `bulk_customer_id` Int64,

    `event_date` DateTime64(6),

    `event_time` DateTime64(6),

    `event_code` String,

    `event_office_facilty_id` String,

    `office_name` String,

    `event_description` String,

    `non_delivery_reason` String
)
AS SELECT DISTINCT
    cxcm.article_number AS article_number,

    cxcm.article_type AS article_type,

    cxcm.booking_date AS booking_date,

    cxcm.booking_time AS booking_time,

    cxcm.booking_office_facility_id AS booking_office_facility_id,

    cxcm.booking_office_name AS booking_office_name,

    cxcm.booking_pin AS booking_pin,

    cxcm.sender_address_city AS sender_address_city,

    cxcm.destination_office_facility_id AS destination_office_facility_id,

    cxcm.destination_office_name AS destination_office_name,

    cxcm.destination_pincode AS destination_pincode,

    cxcm.destination_city AS destination_city,

    cxcm.destination_country AS destination_country,

    cxcm.receiver_name AS receiver_name,

    cxcm.invoice_no AS invoice_no,

    cxcm.line_item AS line_item,

    cxcm.weight_value AS weight_value,

    cxcm.tariff AS tariff,

    cxcm.cod_amount AS cod_amount,

    cxcm.booking_type AS booking_type,

    cxcm.contract_number AS contract_number,

    cxcm.reference AS reference,

    cxcm.bulk_customer_id AS bulk_customer_id,

    te.event_date AS event_date,

    te.event_date AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
LEFT JOIN
(
    SELECT
        article_number,

        event_date,

        event_code,

        office_id,

        office_name,

        event_type,

        delivery_status
    FROM mis_db.new_customer_tracking_event_new_mv
    WHERE event_code = 'ITEM_BOOK'
) AS te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el ON cxcm.bulk_customer_id = el.customer_id
WHERE (cxcm.bulk_customer_id != 0) AND (te.article_number IS NOT NULL);


----------
-- `mis_db`.`amazon_target_table_dt_mv` source

CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv TO mis_db.amazon_target_table_dt
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

    `bulk_customer_id` Int64,

    `event_date` DateTime64(6),

    `event_time` DateTime64(6),

    `event_code` String,

    `event_office_facilty_id` String,

    `office_name` String,

    `event_description` String,

    `non_delivery_reason` String
)
AS 
insert into mis_db.amazon_target_table_dt
SELECT
    cxcm.article_number AS article_number,

    cxcm.article_type AS article_type,

    cxcm.booking_date AS booking_date,

    cxcm.booking_time AS booking_time,

    cxcm.booking_office_facility_id AS booking_office_facility_id,

    cxcm.booking_office_name AS booking_office_name,

    cxcm.booking_pin AS booking_pin,

    cxcm.sender_address_city AS sender_address_city,

    cxcm.destination_office_facility_id AS destination_office_facility_id,

    cxcm.destination_office_name AS destination_office_name,

    cxcm.destination_pincode AS destination_pincode,

    cxcm.destination_city AS destination_city,

    cxcm.destination_country AS destination_country,

    cxcm.receiver_name AS receiver_name,

    cxcm.invoice_no AS invoice_no,

    cxcm.line_item AS line_item,

    cxcm.weight_value AS weight_value,

    cxcm.tariff AS tariff,

    cxcm.cod_amount AS cod_amount,

    cxcm.booking_type AS booking_type,

    cxcm.contract_number AS contract_number,

    cxcm.reference AS reference,

    cxcm.bulk_customer_id AS bulk_customer_id,

    te.event_date AS event_date,

    te.event_date AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
INNER JOIN
(
    SELECT
        t1.article_number,

        t1.event_date,

        t1.event_type,

        t1.event_code,

        t1.office_id,

        t1.office_name,

        t1.delivery_status,

        row_number() OVER (PARTITION BY t1.article_number ORDER BY t1.event_date DESC) AS rn
    FROM mis_db.new_customer_tracking_event_new_mv AS t1
) AS te ON (cxcm.article_number = te.article_number) AND (te.rn = 1)
LEFT JOIN mis_db.customer_log AS el ON cxcm.bulk_customer_id = el.customer_id
WHERE (cxcm.bulk_customer_id != 0) AND (cxcm.booking_date < now())
AND (te.event_date > toDateTime('2025-07-19 00:00:00'))
	    AND (te.event_date < toDateTime('2025-07-22 00:00:00')); 

-----
-- `mis_db`.`mv_pdmanagement_unregisteredarticle_delivered_tracking_event` source

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
------
-- `mis_db`.`mv_pdmanagement_unregisteredarticle_dateentry_tracking_event` source

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

---------------
-- `mis_db`.`mv_pdmanagement_article_recall_tracking_event` source

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

-------------------
-- `mis_db`.`mv_new_pdmanagement_article_transaction_tracking_event_new` source

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
insert into mis_db.new_customer_tracking_event_new_mv
SELECT
    article_number,

    now() AS event_date,

    multiIf(action_code = 1,
 'Item Delivery',
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
----------------------------------
-- `mis_db`.`mv_new_pdmanagement_article_transaction_tracking_event` source

CREATE MATERIALIZED VIEW mis_db.mv_new_pdmanagement_article_transaction_tracking_event TO mis_db.new_customer_tracking_event_mv
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
insert into mis_db.new_customer_tracking_event_mv
SELECT
    article_number,

    now() AS event_date,

    multiIf(action_code = 1,
 'Item Delivery',
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

-------------------------------------
-- `mis_db`.`mv_new_booking_dom_to_tracking_event_new` source

CREATE MATERIALIZED VIEW mis_db.mv_new_booking_dom_to_tracking_event_new TO mis_db.new_customer_tracking_event_new_mv
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
insert into mis_db.new_customer_tracking_event_new_mv
SELECT DISTINCT
    kmd.article_number AS article_number,

    kmd.md_updated_date AS event_date,

    'Item Book' AS event_type,

    'ITEM_BOOK' AS event_code,

    kom.csi_facility_id AS office_id,

    kom.office_name AS office_name,

    'ext_mailbkg_mailbooking_dom' AS source_table,

    '' AS delivery_status,

    1 AS sort_order
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);
-----------------------------------------------------
-- `mis_db`.`mv_new_article_events_tracking_event` source

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
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kae.current_office_id = kom.office_id
WHERE kae.event_date > toDateTime('2025-07-01 00:00:00');


-----------------------------------

-- `mis_db`.`mv_new_article_events_tracking_event_new` source

CREATE MATERIALIZED VIEW mis_db.mv_new_article_events_tracking_event_new TO mis_db.new_customer_tracking_event_new_mv
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

insert into mis_db.new_customer_tracking_event_new_mv
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

------------------------------------------
-- `mis_db`.`mv_new_bag_events_tracking_event_new` source

CREATE MATERIALIZED VIEW mis_db.mv_new_bag_events_tracking_event_new TO mis_db.new_customer_tracking_event_new_mv
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
insert into mis_db.new_customer_tracking_event_new_mv
SELECT DISTINCT
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
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 ON be.to_office_id = kom2.office_id
WHERE (be.transaction_date > toDateTime('2025-07-17 00:00:00'))
  AND (be.transaction_date < toDateTime('2025-07-20 19:00:00'));
--AND (be.transaction_date > toDateTime('2025-07-17 00:00:00'))
--	    AND (be.transaction_date < toDateTime('2025-07-20 19:00:00'));
---------------------------------------
-- `mis_db`.`mv_new_bag_events_tracking_event` source

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
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 ON be.to_office_id = kom2.office_id
WHERE be.transaction_date > toDateTime('2025-07-19 00:00:00');

---------------------------
-- `mis_db`.`mv_new_customer_xml_facility_customer_mv_new` source

CREATE MATERIALIZED VIEW mis_db.mv_new_customer_xml_facility_customer_mv_new TO mis_db.new_customer_xml_facility_customer_new_mv
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
insert into mis_db.new_customer_xml_facility_customer_new_mv
SELECT DISTINCT
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
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON (kmd.sender_address_reference = kba1.address_id) AND (kmd.sender_pincode = kba1.pincode)
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON (kmd.receiver_address_reference = kba2.address_id) AND (kmd.receiver_pincode = kba2.pincode)
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.destination_office_id = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0) AND (kmd.md_updated_date > toDateTime('2025-07-19 00:00:00'))
	    AND (kmd.md_updated_date < toDateTime('2025-07-20 00:00:00')); 
-----------------------
-- `mis_db`.`mv_new_customer_xml_facility_customer_mv` source

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
	SELECT distinct
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
	INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 ON kmd.sender_address_reference = kba1.address_id and kmd.sender_pincode = kba1.pincode
	INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 ON kmd.receiver_address_reference = kba2.address_id and kmd.receiver_pincode = kba2.pincode
	INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
	INNER JOIN mis_db.ext_mdm_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
	INNER JOIN mis_db.ext_mdm_office_master AS kon ON kmd.destination_office_id = kon.office_id
	WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0) AND (kmd.md_updated_date > toDateTime('2025-07-17 00:00:00'))
	    AND (kmd.md_updated_date < toDateTime('2025-07-18 00:00:00')); -- Added condition for less than 20th;