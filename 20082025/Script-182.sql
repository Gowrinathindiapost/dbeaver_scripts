
---not working
--SET join_algorithm = 'parallel_hash';

INSERT INTO mis_db.new_customer_xml_facility_customer_new_mv
SELECT
    kmd.article_number,
    kmd.mail_type_code,
    kmd.md_updated_date,
    kmd.md_updated_date,
    kom.csi_facility_id,
    kom.office_name,
    kmd.origin_pincode,
    kba1.city,
    kon.csi_facility_id,
    kmd.destination_office_name,
    kmd.destination_pincode,
    kba2.city,
    'INDIA',
    kba2.addressee_name,
    kmd.bkg_ref_id,
    '',
    kmd.charged_weight,
    kcd.total_amount,
    kcd.vp_cod_value,
    kmd.booking_type_code,
    kmd.contract_id,
    '',
    kmd.bulk_customer_id
FROM
(
    SELECT
        article_number,
        mail_type_code,
        md_updated_date,
        origin_pincode,
        destination_office_name,
        destination_pincode,
        bkg_ref_id,
        charged_weight,
        booking_type_code,
        contract_id,
        bulk_customer_id,
        sender_address_reference,
        sender_pincode,
        receiver_address_reference,
        receiver_pincode,
        charges_detail_id,
        md_office_id_bkg,
        destination_office_id
    FROM mis_db.ext_mailbkg_mailbooking_dom
    WHERE status_code = 'PC'
      AND _peerdb_is_deleted = 0
      AND bulk_customer_id != 0
      AND md_updated_date >= '2025-08-18 00:00:00'
      AND md_updated_date <  '2025-08-19 00:00:00'
) AS kmd
INNER JOIN
(
    SELECT address_id, pincode, city
    FROM mis_db.ext_mailbkg_booking_address
) AS kba1
    ON (kmd.sender_address_reference = kba1.address_id) AND (kmd.sender_pincode = kba1.pincode)
INNER JOIN
(
    SELECT address_id, pincode, city, addressee_name
    FROM mis_db.ext_mailbkg_booking_address
) AS kba2
    ON (kmd.receiver_address_reference = kba2.address_id) AND (kmd.receiver_pincode = kba2.pincode)
INNER JOIN
(
    SELECT charges_detail_id, total_amount, vp_cod_value
    FROM mis_db.ext_mailbkg_charges_detail
) AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN
(
    SELECT office_id, csi_facility_id, office_name
    FROM mis_db.ext_mdm_office_master
) AS kom
    ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN
(
    SELECT office_id, csi_facility_id, office_name
    FROM mis_db.ext_mdm_office_master
) AS kon
    ON kmd.destination_office_id = kon.office_id
    
    
----    
    SELECT
    kmd.article_number,
    kmd.mail_type_code,
    kmd.md_updated_date,
    kmd.md_updated_date,
    kom.csi_facility_id,
    kom.office_name,
    kmd.origin_pincode,
    kba1.city,
    kon.csi_facility_id,
    kmd.destination_office_name,
    kmd.destination_pincode,
    kba2.city,
    'INDIA' AS country,
    kba2.addressee_name,
    kmd.bkg_ref_id,
    '' AS empty_column1,
    kmd.charged_weight,
    kcd.total_amount,
    kcd.vp_cod_value,
    kmd.booking_type_code,
    kmd.contract_id,
    '' AS empty_column2,
    kmd.bulk_customer_id
FROM
(
    SELECT
        article_number,
        mail_type_code,
        md_updated_date,
        origin_pincode,
        destination_office_name,
        destination_pincode,
        bkg_ref_id,
        charged_weight,
        booking_type_code,
        contract_id,
        bulk_customer_id,
        sender_address_reference,
        sender_pincode,
        receiver_address_reference,
        receiver_pincode,
        charges_detail_id,
        md_office_id_bkg,
        destination_office_id
    FROM mis_db.ext_mailbkg_mailbooking_dom
    WHERE status_code = 'PC'
      AND _peerdb_is_deleted = 0
      AND bulk_customer_id != 0
      AND md_updated_date >= '2025-08-18 00:00:00'
      AND md_updated_date <  '2025-08-19 00:00:00'
) AS kmd
INNER JOIN
(
    SELECT address_id, pincode, city
    FROM mis_db.ext_mailbkg_booking_address
) AS kba1
    ON (kmd.sender_address_reference = kba1.address_id) AND (kmd.sender_pincode = kba1.pincode)
INNER JOIN
(
    SELECT address_id, pincode, city, addressee_name
    FROM mis_db.ext_mailbkg_booking_address
) AS kba2
    ON (kmd.receiver_address_reference = kba2.address_id) AND (kmd.receiver_pincode = kba2.pincode)
INNER JOIN
(
    SELECT charges_detail_id, total_amount, vp_cod_value
    FROM mis_db.ext_mailbkg_charges_detail
) AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN
(
    SELECT office_id, csi_facility_id, office_name
    FROM mis_db.ext_mdm_office_master
) AS kom
    ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN
(
    SELECT office_id, csi_facility_id, office_name
    FROM mis_db.ext_mdm_office_master
) AS kon
    ON kmd.destination_office_id = kon.office_id
