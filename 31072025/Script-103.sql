-- mis_db.mv_new_customer_xml_facility_customer_mv_new source

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
--insert into mis_db.new_customer_xml_facility_customer_new_mv
SELECT DISTINCT
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

--    CAST(kmd.md_updated_date,
-- 'String') AS booking_date,
--
--    CAST(kmd.md_updated_date,
-- 'String') AS booking_time,
    kmd.md_updated_date AS booking_date,
kmd.md_updated_date AS booking_time,


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
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0)
order by booking_date desc;
RY000278810IN
select * from mis_db.mv_new_customer_xml_facility_customer_mv_new where article_number='RY000278810IN'
select * from mis_db.new_customer_tracking_event_new_mv where article_number='AY445932467IN'


SELECT max(md_updated_date), max(_peerdb_synced_at) 
FROM mis_db.ext_mailbkg_mailbooking_dom;


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
WHERE (cxcm.bulk_customer_id != 0) AND (cxcm.booking_date < now()) and event_date > '2025-07-21'