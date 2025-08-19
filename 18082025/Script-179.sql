WITH mail_dom AS (
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
      AND md_updated_date >= '2025-08-14 00:00:00'
      AND md_updated_date <  '2025-08-15 00:00:00'
),
sender_addr AS (
    SELECT
        address_id,
        pincode,
        city
    FROM mis_db.ext_mailbkg_booking_address
),
receiver_addr AS (
    SELECT
        address_id,
        pincode,
        city,
        addressee_name
    FROM mis_db.ext_mailbkg_booking_address
),
charge_det AS (
    SELECT
        charges_detail_id,
        total_amount,
        vp_cod_value
    FROM mis_db.ext_mailbkg_charges_detail
),
booking_office AS (
    SELECT
        office_id,
        csi_facility_id,
        office_name
    FROM mis_db.ext_mdm_office_master
),
destination_office AS (
    SELECT
        office_id,
        csi_facility_id,
        office_name
    FROM mis_db.ext_mdm_office_master
)

INSERT INTO mis_db.new_customer_xml_facility_customer_new_mv
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
FROM mail_dom dom
INNER JOIN sender_addr sa
    ON dom.sender_address_reference = sa.address_id
   AND dom.sender_pincode = sa.pincode
INNER JOIN receiver_addr ra
    ON dom.receiver_address_reference = ra.address_id
   AND dom.receiver_pincode = ra.pincode
INNER JOIN charge_det cd
    ON dom.charges_detail_id = cd.charges_detail_id
INNER JOIN booking_office bok
    ON dom.md_office_id_bkg = bok.office_id
INNER JOIN destination_office dof
    ON dom.destination_office_id = dof.office_id;
