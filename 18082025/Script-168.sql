-- mis_db.amazon_target_table_dt_ib_mv source

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

    toDateTime64(te.event_date,
 6) + toIntervalSecond(0) AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
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
    FROM mis_db.new_customer_tracking_event_mv
    WHERE event_code = 'ITEM_BOOK'
) AS te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el ON cxcm.bulk_customer_id = el.customer_id
WHERE (cxcm.bulk_customer_id != 0) AND (te.article_number IS NOT NULL)
and cxcm.bulk_customer_id=1000002954


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