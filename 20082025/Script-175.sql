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

insert into mis_db.new_customer_xml_facility_customer_new_mv
SELECT 
    kmd.article_number AS article_number,

    kmd.mail_type_code AS article_type,

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
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0) AND (kmd.bulk_customer_id!=0)
AND kmd.md_updated_date >= '2025-08-18 00:00:00'
  AND kmd.md_updated_date <  '2025-08-19 00:00:00';

--AND kmd.md_updated_date >= '2025-07-17 00:00:00'
--  AND kmd.md_updated_date <  '2025-07-17 13:00:00';
    --2
    
    
  