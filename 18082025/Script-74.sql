834339981609968

select * from ext_mailbkg_booking_address where address_id='834339981609968'


CREATE MATERIALIZED VIEW mis_db.mv_new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R
TO mis_db.new_customer_xml_facility_customer_mv
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
AS SELECT
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
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0)
and kmd.md_updated_date > toDateTime('2025-07-01 00:00:00')
and kmd.article_number='PP576355033IN'

SELECT *
FROM mis_db.ext_mailbkg_booking_address AS e2
WHERE address_id IN (
    SELECT sender_address_reference FROM mis_db.ext_mailbkg_mailbooking_dom
    WHERE article_number = 'PP576355033IN' AND status_code = 'PC'
)
OR address_id IN (
    SELECT receiver_address_reference FROM mis_db.ext_mailbkg_mailbooking_dom
    WHERE article_number = 'PP576355033IN' AND status_code = 'PC'
)


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
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd final
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 final ON kmd.sender_address_reference = kba1.address_id and kmd.sender_pincode = kba1.pincode
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 final ON kmd.receiver_address_reference = kba2.address_id and kmd.receiver_pincode = kba2.pincode
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd final ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom final ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon final ON kmd.destination_office_id = kon.office_id
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);

select distinct(article_number),* from mis_db.ext_mailbkg_mailbooking_dom where `bulk_customer_id` <> 0 and status_code='PC'--4,477,138--4,509,823

optimize TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.new_customer_xml_facility_customer_mv ON CLUSTER cluster_1S_2R;
