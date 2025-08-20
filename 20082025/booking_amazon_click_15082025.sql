insert into mis_db.new_customer_xml_facility_customer_new_mv
SELECT DISTINCT
    kmd.article_number,
    kmd.mail_type_code AS article_type,
    kmd.md_updated_date AS booking_date,
    kmd.md_updated_date AS booking_time,
    kom.csi_facility_id AS booking_office_facility_id,
    kom.office_name AS booking_office_name,
    kmd.origin_pincode AS booking_pin,
    kba1.city AS sender_address_city,
    kon.csi_facility_id AS destination_office_facility_id,
    kmd.destination_office_name,
    kmd.destination_pincode,
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
    kmd.bulk_customer_id
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
/* Using filtered subqueries for joins to reduce memory usage */
INNER JOIN (
    SELECT address_id, pincode, city 
    FROM mis_db.ext_mailbkg_booking_address 
    WHERE _peerdb_is_deleted = 0
) AS kba1 ON 
    kmd.sender_address_reference = kba1.address_id AND 
    kmd.sender_pincode = kba1.pincode
INNER JOIN (
    SELECT address_id, pincode, city, addressee_name 
    FROM mis_db.ext_mailbkg_booking_address 
    WHERE _peerdb_is_deleted = 0
) AS kba2 ON 
    kmd.receiver_address_reference = kba2.address_id AND 
    kmd.receiver_pincode = kba2.pincode
INNER JOIN (
    SELECT charges_detail_id, total_amount, vp_cod_value 
    FROM mis_db.ext_mailbkg_charges_detail 
    WHERE _peerdb_is_deleted = 0
) AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN (
    SELECT office_id, csi_facility_id, office_name 
    FROM mis_db.ext_mdm_office_master 
    WHERE _peerdb_is_deleted = 0
) AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN (
    SELECT office_id, csi_facility_id 
    FROM mis_db.ext_mdm_office_master 
    WHERE _peerdb_is_deleted = 0
) AS kon ON kmd.destination_office_id = kon.office_id
WHERE kmd.status_code = 'PC'
AND kmd._peerdb_is_deleted = 0
AND kmd.md_updated_date BETWEEN parseDateTime64BestEffort('2025-08-14 00:00:00.000000') 
                         AND parseDateTime64BestEffort('2025-08-14 23:59:59.000000')
AND kmd.bulk_customer_id != 0
--JM322186472IN
