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

    '' AS cod_amount,--cod amount not streamed in kafka

    '' AS booking_type,-- booking type code not streamed in kafka

    '' AS contract_number,-- contract id not streamed in kafka

    '' AS reference,

    kmd.bulk_customer_id AS bulk_customer_id
FROM trackandtrace.kafka_mailbooking_dom AS kmd
INNER JOIN trackandtrace.kafka_booking_address AS kba1 ON (kmd.sender_address_reference = kba1.address_id) AND (kmd.sender_pincode = kba1.pincode)
INNER JOIN trackandtrace.kafka_booking_address AS kba2 ON (kmd.receiver_address_reference = kba2.address_id) AND (kmd.destination_pincode = kba2.pincode)
INNER JOIN trackandtrace.kafka_charges_detail AS kcd ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN trackandtrace.kafka_office_master AS kom ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN trackandtrace.kafka_office_master AS kon ON kmd.destination_office_id = kon.office_id
WHERE (kmd.status_code = 'PC') 