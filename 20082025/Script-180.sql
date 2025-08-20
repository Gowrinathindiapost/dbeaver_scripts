INSERT INTO mis_db.new_customer_xml_facility_customer_new_mv
WITH filtered_mail_dom AS (
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
    FROM mis_db.ext_mailbkg_mailbooking_dom FINAL
    WHERE status_code = 'PC'
      AND _peerdb_is_deleted = 0
      AND bulk_customer_id != 0
      AND md_updated_date >= '2025-08-18 00:00:00'
      AND md_updated_date < '2025-08-19 00:00:00'
)
SELECT
    dom.article_number AS article_number,
    dom.mail_type_code AS article_type,
    dom.md_updated_date AS booking_date,
    dom.md_updated_date AS booking_time,
    bok.csi_facility_id AS booking_office_facility_id,
    bok.office_name AS booking_office_name,
    dom.origin_pincode AS booking_pin,
    sa.city AS sender_address_city,
    dof.csi_facility_id AS destination_office_facility_id,
    dom.destination_office_name AS destination_office_name,
    dom.destination_pincode AS destination_pincode,
    ra.city AS destination_city,
    'INDIA' AS destination_country,
    ra.addressee_name AS receiver_name,
    dom.bkg_ref_id AS invoice_no,
    '' AS line_item,
    dom.charged_weight AS weight_value,
    cd.total_amount AS tariff,
    cd.vp_cod_value AS cod_amount,
    dom.booking_type_code AS booking_type,
    dom.contract_id AS contract_number,
    '' AS reference,
    dom.bulk_customer_id AS bulk_customer_id
FROM filtered_mail_dom dom
INNER JOIN (SELECT * FROM mis_db.ext_mailbkg_booking_address FINAL) sa
    ON dom.sender_address_reference = sa.address_id
    AND dom.sender_pincode = sa.pincode
INNER JOIN (SELECT * FROM mis_db.ext_mailbkg_booking_address FINAL) ra
    ON dom.receiver_address_reference = ra.address_id
    AND dom.receiver_pincode = ra.pincode
INNER JOIN (SELECT * FROM mis_db.ext_mailbkg_charges_detail FINAL) cd
    ON dom.charges_detail_id = cd.charges_detail_id
INNER JOIN (SELECT * FROM mis_db.ext_mdm_office_master FINAL) bok
    ON dom.md_office_id_bkg = bok.office_id
INNER JOIN (SELECT * FROM mis_db.ext_mdm_office_master FINAL) dof
    ON dom.destination_office_id = dof.office_id;


--WITH mail_booking_data AS (
--    SELECT
--        kmd.article_number,
--        kmd.mail_type_code,
--        kmd.md_updated_date,
--        kmd.origin_pincode,
--        kmd.destination_pincode,
--        kmd.destination_office_name,
--        kmd.bkg_ref_id,
--        kmd.charged_weight,
--        kmd.booking_type_code,
--        kmd.contract_id,
--        kmd.bulk_customer_id,
--        kmd.sender_address_reference,
--        kmd.sender_pincode,
--        kmd.receiver_address_reference,
--        kmd.receiver_pincode,
--        kmd.charges_detail_id,
--        kmd.md_office_id_bkg,
--        kmd.destination_office_id
--    FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd final
--    WHERE kmd.status_code = 'PC' 
--      AND kmd._peerdb_is_deleted = 0 
--      AND kmd.bulk_customer_id != 0
--      AND kmd.md_updated_date >= '2025-08-14 00:00:00'
--      AND kmd.md_updated_date < '2025-08-15 00:00:00'
--)
--
--INSERT INTO mis_db.new_customer_xml_facility_customer_new_mv
--SELECT
--    mbd.article_number AS article_number,
--    mbd.mail_type_code AS article_type,
--    mbd.md_updated_date AS booking_date,
--    mbd.md_updated_date AS booking_time,
--    kom.csi_facility_id AS booking_office_facility_id,
--    kom.office_name AS booking_office_name,
--    mbd.origin_pincode AS booking_pin,
--    kba1.city AS sender_address_city,
--    kon.csi_facility_id AS destination_office_facility_id,
--    mbd.destination_office_name AS destination_office_name,
--    mbd.destination_pincode AS destination_pincode,
--    kba2.city AS destination_city,
--    'INDIA' AS destination_country,
--    kba2.addressee_name AS receiver_name,
--    mbd.bkg_ref_id AS invoice_no,
--    '' AS line_item,
--    mbd.charged_weight AS weight_value,
--    kcd.total_amount AS tariff,
--    kcd.vp_cod_value AS cod_amount,
--    mbd.booking_type_code AS booking_type,
--    mbd.contract_id AS contract_number,
--    '' AS reference,
--    mbd.bulk_customer_id AS bulk_customer_id
--FROM mail_booking_data mbd
--INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 final 
--    ON (mbd.sender_address_reference = kba1.address_id) 
--    AND (mbd.sender_pincode = kba1.pincode)
--INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 final 
--    ON (mbd.receiver_address_reference = kba2.address_id) 
--    AND (mbd.receiver_pincode = kba2.pincode)
--INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd final 
--    ON mbd.charges_detail_id = kcd.charges_detail_id
--INNER JOIN mis_db.ext_mdm_office_master AS kom final 
--    ON mbd.md_office_id_bkg = kom.office_id
--INNER JOIN mis_db.ext_mdm_office_master AS kon final 
--    ON mbd.destination_office_id = kon.office_id;