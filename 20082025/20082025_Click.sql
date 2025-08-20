

INSERT INTO mis_db.amazon_target_table_dt (
        article_number, article_type, booking_date, booking_time,
        booking_office_facility_id, booking_office_name, booking_pin, sender_address_city,
        destination_office_facility_id, destination_office_name, destination_pincode, 
        destination_city, destination_country, receiver_name, invoice_no, line_item,
        weight_value, tariff, cod_amount, booking_type, contract_number, reference,
        bulk_customer_id, event_date, event_time, event_code, event_office_facilty_id,
        office_name, event_description, non_delivery_reason
    )
    SELECT DISTINCT
        cxcm.article_number, 
        cxcm.article_type,
        cxcm.booking_date,
    cxcm.booking_time,
        cxcm.booking_office_facility_id,
        cxcm.booking_office_name, 
        cxcm.booking_pin, 
        cxcm.sender_address_city, 
        cxcm.destination_office_facility_id,
        cxcm.destination_office_name, 
        cxcm.destination_pincode, 
        cxcm.destination_city, 
        cxcm.destination_country,
        cxcm.receiver_name, 
        cxcm.invoice_no, 
        cxcm.line_item, 
        cxcm.weight_value, 
        cxcm.tariff, 
        cxcm.cod_amount,
        cxcm.booking_type, 
        cxcm.contract_number, 
        cxcm.reference, 
        cxcm.bulk_customer_id,
      te.event_date AS event_date,
    te.event_date AS event_time,
        te.event_code,
        te.office_id AS event_office_facilty_id,
        te.office_name,
        te.event_type AS event_description,
        te.delivery_status AS non_delivery_reason
    FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
    INNER JOIN (
        SELECT 
            t1.article_number, 
            t1.event_date, 
            t1.event_type, 
            t1.event_code,
            t1.office_id, 
            t1.office_name, 
            t1.delivery_status
        FROM mis_db.new_customer_tracking_event_mv AS t1
        INNER JOIN (
            SELECT 
                article_number, 
                max(event_date) AS max_event_date
            FROM mis_db.new_customer_tracking_event_mv
            GROUP BY article_number
        ) AS t2 ON t1.article_number = t2.article_number 
                AND t1.event_date = t2.max_event_date
    ) te ON cxcm.article_number = te.article_number
    LEFT JOIN mis_db.customer_log AS el
        ON cxcm.bulk_customer_id = el.customer_id
    WHERE cxcm.bulk_customer_id != 0
        AND cxcm.booking_date < now()
        
        AND te.event_date >= toDateTime64('2025-08-17 00:00:00.000000', 6)
  AND te.event_date < toDateTime64('2025-08-17 23:59:59.000000', 6)
  
  AND CAST(te.event_date AS Date) >= CAST(now() - INTERVAL 2 DAY AS Date)

-------------------below worked
SELECT
    '<?xml version="1.0" encoding="UTF-8"?>'||
    '<LatestEventDetails>' ||
    arrayStringConcat(
        arrayMap(
            x -> '<ArticleDetails>' ||
                '<ArticleNumber>' || toString(x.1) || '</ArticleNumber>' ||
                '<ArticleType>' || upper(toString(x.2)) || '</ArticleType>' ||
                '<BookingDate>' || toString(x.3) || '</BookingDate>' ||
                '<BookingTime>' || toString(x.4) || '</BookingTime>' ||
                '<BookingOfficeFacilityID>' || upper(toString(x.5)) || '</BookingOfficeFacilityID>' ||
                '<BookingOfficeName>' || upper(toString(x.6)) || '</BookingOfficeName>' ||
                '<BookingPIN>' || toString(x.7) || '</BookingPIN>' ||
                '<SenderAddressCity>' || upper(toString(x.8)) || '</SenderAddressCity>' ||
                '<DestinationOfficeFacilityID>' || upper(toString(x.9)) || '</DestinationOfficeFacilityID>' ||
                '<DestinationOfficeName>' || upper(toString(x.10)) || '</DestinationOfficeName>' ||
                '<DestinationPIN>' || toString(x.11) || '</DestinationPIN>' ||
                '<DestinationCity>' || upper(toString(x.12)) || '</DestinationCity>' ||
                '<DestinationCountry>' || upper(toString(x.13)) || '</DestinationCountry>' ||
                '<ReceiverName>' || upper(replaceRegexpAll(toString(x.14), '[^a-zA-Z0-9 ]', '')) || '</ReceiverName>' ||
                '<InvoiceNo>' || toString(x.15) || '</InvoiceNo>' ||
                '<LineItem/>' ||
                '<WeightValue>' || toString(x.16) || '</WeightValue>' ||
                '<Tariff>' || toString(x.17) || '</Tariff>' ||
                '<CODAmount>' || toString(x.18) || '</CODAmount>' ||
                '<BookingType>' || upper(toString(x.19)) || '</BookingType>' ||
                '<ContractNumber>' || toString(x.20) || '</ContractNumber>' ||
                '<Refrence/>' ||
                '<EventCode>' || upper(toString(x.21)) || '</EventCode>' ||
                '<EventDescription>' || upper(toString(x.22)) || '</EventDescription>' ||
                '<EventOfficeFacilityID>' || upper(toString(x.23)) || '</EventOfficeFacilityID>' ||
                '<EventOfficeName>' || upper(toString(x.24)) || '</EventOfficeName>' ||
                '<EventDate>' || toString(x.25) || '</EventDate>' ||
                '<EventTime>' || toString(x.26) || '</EventTime>' ||
                '<NonDelReason>' || upper(toString(x.27)) || '</NonDelReason>' ||
                '</ArticleDetails>',
            groupArray((
                cxcm.article_number,
                cxcm.article_type,
                cxcm.booking_date,
                cxcm.booking_time,
                cxcm.booking_office_facility_id,
                cxcm.booking_office_name,
                cxcm.booking_pin,
                cxcm.sender_address_city,
                cxcm.destination_office_facility_id,
                cxcm.destination_office_name,
                cxcm.destination_pincode,
                cxcm.destination_city,
                cxcm.destination_country,
                cxcm.receiver_name,
                cxcm.invoice_no,
                cxcm.weight_value,
                cxcm.tariff,
                cxcm.cod_amount,
                cxcm.booking_type,
                cxcm.contract_number,
                cxcm.event_code,
                cxcm.event_description,
                cxcm.event_office_facilty_id,
                cxcm.office_name,
                cxcm.event_date,
                cxcm.event_time,
                ifNull(cxcm.non_delivery_reason, '')
            ))
        )
    ) ||
    '</LatestEventDetails>' AS xml_output
FROM (
    SELECT DISTINCT
        cxcm.article_number, cxcm.article_type,
        formatDateTime(cxcm.booking_date, '%d%m%Y') AS booking_date,
        formatDateTime(cxcm.booking_time, '%H%i%s') AS booking_time,
        cxcm.booking_office_facility_id,
        cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
        cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
        cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
        cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
        formatDateTime(te.event_date, '%d%m%Y') AS event_date,
        formatDateTime(te.event_date, '%H%i%s') AS event_time,
        te.event_code AS event_code,
        te.office_id AS event_office_facilty_id,
        te.office_name AS office_name,
        te.event_type AS event_description,
        te.delivery_status as non_delivery_reason
    FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
    INNER JOIN mis_db.new_customer_tracking_event_mv te
        ON cxcm.article_number = te.article_number
    LEFT JOIN mis_db.customer_log AS el
        ON cxcm.bulk_customer_id = el.customer_id
    WHERE cxcm.bulk_customer_id = 2000014100 --2000014100 --2000008005 --2000014074 --3000063972 --1000002954
      AND cxcm.booking_date >= toDateTime64('2025-08-18 00:00:00.000000', 6)
      AND cxcm.booking_date <= toDateTime64('2025-08-18 23:59:59.000000', 6)
      AND te.event_date >= toDateTime64('2025-08-18 00:00:00.000000', 6)
      AND te.event_date <= toDateTime64('2025-08-18 23:59:59.000000', 6)
      AND event_code= 'ITEM_BOOK'
) AS cxcm
----ran second 16:08

insert into mis_db.new_customer_tracking_event_mv
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
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0) AND (kmd.bulk_customer_id != 0)
and kmd.md_updated_date >='2025-08-01'
---below is working super  ran this first at 16:06
INSERT INTO mis_db.new_customer_xml_facility_customer_mv
WITH
    -- Booking table with filters pushed down
    booking AS (
        SELECT article_number, mail_type_code, md_updated_date,
               origin_pincode, destination_pincode, destination_office_id,
               destination_office_name, bkg_ref_id, charged_weight,
               booking_type_code, contract_id, bulk_customer_id,
               sender_address_reference, sender_pincode,
               receiver_address_reference, receiver_pincode, charges_detail_id,
               md_office_id_bkg
        FROM mis_db.ext_mailbkg_mailbooking_dom
        WHERE status_code = 'PC'
          AND _peerdb_is_deleted = 0
          AND md_updated_date >= '2025-08-01 00:00:00'
          AND md_updated_date <  '2025-08-12 00:00:00'
          AND bulk_customer_id IN (1000002954,2000008005, 2000014074, 2000014100, 3000063972)
    )
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
    kmd.booking_type_code,
    kmd.contract_id,
    '' AS reference,
    kmd.bulk_customer_id
FROM booking AS kmd
-- Sender address join, filtered
INNER JOIN (
    SELECT address_id, pincode, city, addressee_name
    FROM mis_db.ext_mailbkg_booking_address
    WHERE (address_id, pincode) IN (
        SELECT DISTINCT sender_address_reference, sender_pincode
        FROM booking
    )
) AS kba1
    ON kmd.sender_address_reference = kba1.address_id
   AND kmd.sender_pincode = kba1.pincode
-- Receiver address join, filtered
INNER JOIN (
    SELECT address_id, pincode, city, addressee_name
    FROM mis_db.ext_mailbkg_booking_address
    WHERE (address_id, pincode) IN (
        SELECT DISTINCT receiver_address_reference, receiver_pincode
        FROM booking
    )
) AS kba2
    ON kmd.receiver_address_reference = kba2.address_id
   AND kmd.receiver_pincode = kba2.pincode
-- Charges join, filtered
INNER JOIN (
    SELECT charges_detail_id, total_amount, vp_cod_value
    FROM mis_db.ext_mailbkg_charges_detail
    WHERE charges_detail_id IN (
        SELECT DISTINCT charges_detail_id
        FROM booking
    )
) AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
-- Booking office join, filtered
INNER JOIN (
    SELECT office_id, office_name, csi_facility_id
    FROM mis_db.ext_mdm_office_master
    WHERE office_id IN (
        SELECT DISTINCT md_office_id_bkg
        FROM booking
    )
) AS kom
    ON kmd.md_office_id_bkg = kom.office_id
-- Destination office join, filtered
INNER JOIN (
    SELECT office_id, office_name, csi_facility_id
    FROM mis_db.ext_mdm_office_master
    WHERE office_id IN (
        SELECT DISTINCT destination_office_id
        FROM booking
    )
) AS kon
    ON kmd.destination_office_id = kon.office_id;

---------------
INSERT INTO mis_db.new_customer_xml_facility_customer_mv
SELECT DISTINCT
    kmd.article_number,
    kmd.mail_type_code AS article_type,
    kmd.md_updated_date AS booking_date,
    kmd.md_updated_date AS booking_time,
    kom.csi_facility_id AS booking_office_facility_id,
    kom.office_name     AS booking_office_name,
    kmd.origin_pincode AS booking_pin,
    kba1.city          AS sender_address_city,
    kon.csi_facility_id AS destination_office_facility_id,
    kmd.destination_office_name,
    kmd.destination_pincode,
    kba2.city          AS destination_city,
    'INDIA'            AS destination_country,
    kba2.addressee_name AS receiver_name,
    kmd.bkg_ref_id     AS invoice_no,
    ''                 AS line_item,
    kmd.charged_weight AS weight_value,
    kcd.total_amount   AS tariff,
    kcd.vp_cod_value   AS cod_amount,
    kmd.booking_type_code,
    kmd.contract_id,
    ''                 AS reference,
    kmd.bulk_customer_id
FROM (
    SELECT article_number, mail_type_code, md_updated_date,
           origin_pincode, destination_pincode, destination_office_id,
           destination_office_name, bkg_ref_id, charged_weight,
           booking_type_code, contract_id, bulk_customer_id,
           sender_address_reference, sender_pincode,
           receiver_address_reference, receiver_pincode,
           charges_detail_id, md_office_id_bkg
    FROM mis_db.ext_mailbkg_mailbooking_dom
    WHERE status_code = 'PC'
      AND _peerdb_is_deleted = 0
      AND md_updated_date >= '2025-08-19 00:00:00'
      AND md_updated_date <  '2025-08-19 08:00:00'
      AND bulk_customer_id IN (1000002954)
) AS kmd
INNER JOIN (
    SELECT address_id, pincode, city, addressee_name
    FROM mis_db.ext_mailbkg_booking_address
) AS kba1
    ON kmd.sender_address_reference = kba1.address_id
   AND kmd.sender_pincode = kba1.pincode
INNER JOIN (
    SELECT address_id, pincode, city, addressee_name
    FROM mis_db.ext_mailbkg_booking_address
) AS kba2
    ON kmd.receiver_address_reference = kba2.address_id
   AND kmd.receiver_pincode = kba2.pincode
INNER JOIN (
    SELECT charges_detail_id, total_amount, vp_cod_value
    FROM mis_db.ext_mailbkg_charges_detail
) AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN (
    SELECT office_id, office_name, csi_facility_id
    FROM mis_db.ext_mdm_office_master
) AS kom
    ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN (
    SELECT office_id, csi_facility_id
    FROM mis_db.ext_mdm_office_master
) AS kon
    ON kmd.destination_office_id = kon.office_id;

--------------with any inner jion below works
INSERT INTO mis_db.new_customer_xml_facility_customer_mv
WITH
    -- Only required fields from address table
    addrss AS (
        SELECT address_id, pincode, city, addressee_name
        FROM mis_db.ext_mailbkg_booking_address
    ),
    -- Only required fields from charges detail
    charges AS (
        SELECT charges_detail_id, total_amount, vp_cod_value
        FROM mis_db.ext_mailbkg_charges_detail
    ),
    -- Office master (reused for booking + destination offices)
    ofcmaster AS (
        SELECT office_id, office_name, csi_facility_id
        FROM mis_db.ext_mdm_office_master
    ),
    -- Booking table with filters pushed down
    booking AS (
        SELECT article_number, mail_type_code, md_updated_date,
               origin_pincode, destination_pincode, destination_office_id,
               destination_office_name, bkg_ref_id, charged_weight,
               booking_type_code, contract_id, bulk_customer_id,
               sender_address_reference, sender_pincode,
               receiver_address_reference, receiver_pincode, charges_detail_id,
               md_office_id_bkg
        FROM mis_db.ext_mailbkg_mailbooking_dom
        WHERE status_code = 'PC'
          AND _peerdb_is_deleted = 0
          AND md_updated_date >= '2025-08-19 00:00:00'
          AND md_updated_date <  '2025-08-20 00:00:00'
          AND bulk_customer_id IN (1000002954)
    )
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
    kmd.booking_type_code,
    kmd.contract_id,
    '' AS reference,
    kmd.bulk_customer_id
FROM booking AS kmd
INNER JOIN addrss AS kba1
    ON kmd.sender_address_reference = kba1.address_id
   AND kmd.sender_pincode = kba1.pincode
INNER JOIN addrss AS kba2
    ON kmd.receiver_address_reference = kba2.address_id
   AND kmd.receiver_pincode = kba2.pincode
INNER JOIN charges AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN ofcmaster AS kom
    ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN ofcmaster AS kon
    ON kmd.destination_office_id = kon.office_id


-----------------
insert into mis_db.new_customer_xml_facility_customer_mv
 SELECT DISTINCT
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
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0)
AND kmd.md_updated_date >= '2025-08-19 00:00:00'   -- ✅ date filter start
  AND kmd.md_updated_date <  '2025-08-20 00:00:00'   -- ✅ date filter end (exclusive)
  AND kmd.bulk_customer_id IN (1000002954) , 2000008005, 2000014074, 2000014100, 3000063972);


-------------
INSERT INTO mis_db.new_customer_xml_facility_customer_mv
WITH
    -- Only keep required fields from address table
    addrss AS (
        SELECT address_id, pincode, city, addressee_name
        FROM mis_db.ext_mailbkg_booking_address
    ),
    -- Only keep required fields from charges detail
    charges AS (
        SELECT charges_detail_id, total_amount, vp_cod_value
        FROM mis_db.ext_mailbkg_charges_detail
        WHERE md_cd_updated_date >= '2025-08-18 00:00:00'
          AND md_cd_updated_date <  '2025-08-19 00:00:00'
    ),
    -- Only keep required fields from office master
    ofcmaster AS (
        SELECT office_id, office_name, csi_facility_id
        FROM mis_db.ext_mdm_office_master
    ),
    -- Only required fields from intl mailbooking
    booking AS (
        SELECT article_number, mail_type_code, md_updated_date, 
               origin_pincode, destination_cname, bkg_ref_id, charged_weight,
               booking_type_code, contract_id, bulk_customer_id,
               sender_address_reference, sender_pincode,
               receiver_address_reference, charges_detail_id, md_office_id_bkg
        FROM mis_db.ext_mailbkg_mailbooking_intl
        WHERE status_code = 'PC'
          AND _peerdb_is_deleted = 0
          AND md_updated_date >= '2025-08-18 00:00:00'
          AND md_updated_date <  '2025-08-19 00:00:00'
          AND bulk_customer_id IN (1000002954, 2000008005, 2000014074, 2000014100, 3000063972)
    )
SELECT DISTINCT
    kmd.article_number AS article_number,
    kmd.mail_type_code AS article_type,
    kmd.md_updated_date AS booking_date,
    kmd.md_updated_date AS booking_time,
    kom.csi_facility_id AS booking_office_facility_id,
    kom.office_name AS booking_office_name,
    kmd.origin_pincode AS booking_pin,
    kba1.city AS sender_address_city,
    '' AS destination_office_facility_id,
    '' AS destination_office_name,
    0 AS destination_pincode,
    '' AS destination_city,
    kmd.destination_cname AS destination_country,
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
FROM booking AS kmd
INNER JOIN addrss AS kba1 
    ON kmd.sender_address_reference = kba1.address_id 
   AND kmd.sender_pincode = kba1.pincode
INNER JOIN addrss AS kba2 
    ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN charges AS kcd 
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN ofcmaster AS kom 
    ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN ofcmaster AS kon 
    ON kmd.md_office_id_bkg = kon.office_id

-------below didnot work
INSERT INTO mis_db.new_customer_xml_facility_customer_mv
SELECT --DISTINCT
    kmd.article_number AS article_number,
    kmd.mail_type_code AS article_type,
    kmd.md_updated_date AS booking_date,
    kmd.md_updated_date AS booking_time,
    kom.csi_facility_id AS booking_office_facility_id,
    kom.office_name AS booking_office_name,
    kmd.origin_pincode AS booking_pin,
    kba1.city AS sender_address_city,
    '' AS destination_office_facility_id,
    '' AS destination_office_name,
    0 AS destination_pincode,
    '' AS destination_city,
    kmd.destination_cname AS destination_country,
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
FROM mis_db.ext_mailbkg_mailbooking_intl AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 
    ON (kmd.sender_address_reference = kba1.address_id) 
   AND (kmd.sender_pincode = kba1.pincode)
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 
    ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd 
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom 
    ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN mis_db.ext_mdm_office_master AS kon 
    ON kmd.md_office_id_bkg = kon.office_id
WHERE kmd.status_code = 'PC'
  AND kmd._peerdb_is_deleted = 0
  AND kmd.md_updated_date >= '2025-08-19 00:00:00'   -- ✅ date filter start
  AND kmd.md_updated_date <  '2025-08-20 00:00:00'   -- ✅ date filter end (exclusive)
  AND kmd.bulk_customer_id IN (1000002954) , 2000008005, 2000014074, 2000014100, 3000063972);  -- ✅ customer filter



-------below not working
INSERT INTO mis_db.new_customer_xml_facility_customer_new_mv

WITH addrss AS (
    SELECT address_id, pincode, city, addressee_name 
    FROM mis_db.ext_mailbkg_booking_address FINAL
),
ofcmaster AS (
    SELECT office_id, office_name, csi_facility_id 
    FROM mis_db.dim_mdm_office FINAL
)

SELECT DISTINCT     
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
FROM (
    SELECT booking_type_code, contract_id, bulk_customer_id, charged_weight, destination_pincode,
           destination_office_name, origin_pincode, md_updated_date, article_number, mail_type_code,
           bkg_ref_id, sender_address_reference, sender_pincode, receiver_address_reference,
           receiver_pincode, charges_detail_id, md_office_id_bkg, destination_office_id
    FROM mis_db.ext_mailbkg_mailbooking_dom FINAL 
    WHERE status_code = 'PC'
      AND _peerdb_is_deleted = 0
      AND md_updated_date >= '2025-08-19 00:00:00' 
      AND md_updated_date <  '2025-08-20 00:00:00'
      AND bulk_customer_id = 1000002954            -- ✅ added condition
     -- AND bulk_customer_id IN (1000002954)--, 2000008005, 2000014074, 2000014100, 3000063972)
) AS kmd 
INNER JOIN addrss AS kba1 
    ON (kmd.sender_address_reference = kba1.address_id) 
   AND (kmd.sender_pincode = kba1.pincode) 
INNER JOIN addrss AS kba2 
    ON (kmd.receiver_address_reference = kba2.address_id) 
   AND (kmd.receiver_pincode = kba2.pincode) 
INNER JOIN (
    SELECT charges_detail_id, vp_cod_value, total_amount  
    FROM mis_db.ext_mailbkg_charges_detail FINAL 
    WHERE md_cd_updated_date >= '2025-08-19 00:00:00' 
      AND md_cd_updated_date <  '2025-08-20 00:00:00'
) AS kcd 
    ON kmd.charges_detail_id = kcd.charges_detail_id 
INNER JOIN ofcmaster AS kom 
    ON kmd.md_office_id_bkg = kom.office_id 
INNER JOIN ofcmaster AS kon 
    ON kmd.destination_office_id = kon.office_id 
ORDER BY booking_date DESC;



-----query 3 below working with date
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
where kae.event_date ='2025-08-01'

---query 2 bagging events
--memorey error without distinct
insert into mis_db.new_customer_tracking_event_mv
SELECT --DISTINCT
    coalesce(bcc.article_number, boc.article_number) AS article_number,
    be.transaction_date AS event_date,
    multiIf(
        be.event_type = 'CL', 'Bag Close',
        be.event_type = 'DI', 'Bag Dispatch',
        be.event_type = 'RO', 'Item Received',
        be.event_type IN ('OP','OR'), 'Bag Open',
        ''
    ) AS event_type,
    multiIf(
        be.event_type = 'CL', 'BAG_CLOSE',
        be.event_type = 'DI', 'BAG_DISPATCH',
        be.event_type = 'RO', 'TMO_RECEIVE',
        be.event_type IN ('OP','OR'), 'BAG_OPEN',
        ''
    ) AS event_code,
    coalesce(kom.csi_facility_id, kom2.csi_facility_id) AS office_id,
    coalesce(kom.office_name, kom2.office_name) AS office_name,
    'ext_bagmgmt_bag_event' AS source_table,
    '' AS delivery_status,
    3 AS sort_order
FROM mis_db.ext_bagmgmt_bag_event AS be
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content AS bcc 
    ON be.bag_number = bcc.bag_number 
    AND be.event_type IN ('CL','DI')
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content AS boc 
    ON be.bag_number = boc.bag_number 
    AND be.event_type IN ('OP','OR')
LEFT JOIN mis_db.ext_mdm_office_master AS kom 
    ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 
    ON be.to_office_id = kom2.office_id
WHERE be.transaction_date >= toDate('2025-08-18');   -- ✅ correct place


--memorey error
insert into mis_db.new_customer_tracking_event_mv
SELECT 
    coalesce(bcc.article_number, boc.article_number) AS article_number,
    be.transaction_date AS event_date,

    multiIf(
        be.event_type = 'CL', 'Bag Close',
        be.event_type = 'DI', 'Bag Dispatch',
        be.event_type = 'RO', 'Item Received',
        be.event_type IN ('OP','OR'), 'Bag Open',
        ''
    ) AS event_type,

    multiIf(
        be.event_type = 'CL', 'BAG_CLOSE',
        be.event_type = 'DI', 'BAG_DISPATCH',
        be.event_type = 'RO', 'TMO_RECEIVE',
        be.event_type IN ('OP','OR'), 'BAG_OPEN',
        ''
    ) AS event_code,

    coalesce(kom.csi_facility_id, kom2.csi_facility_id) AS office_id,
    coalesce(kom.office_name, kom2.office_name) AS office_name,

    'ext_bagmgmt_bag_event' AS source_table,
    '' AS delivery_status,
    3 AS sort_order
FROM mis_db.ext_bagmgmt_bag_event AS be
LEFT JOIN mis_db.ext_bagmgmt_bag_close_content AS bcc 
    ON be.bag_number = bcc.bag_number
    AND be.event_type IN ('CL','DI')
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content AS boc 
    ON be.bag_number = boc.bag_number
    AND be.event_type IN ('OP','OR')
LEFT JOIN mis_db.ext_mdm_office_master AS kom 
    ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 
    ON be.to_office_id = kom2.office_id
WHERE 
    be.transaction_date >= toDate('2025-08-18')  -- ✅ date filter
    AND be.event_type IN ('CL','DI','RO','OP','OR');  -- ✅ filter pushed down

----memory error --below worked with dates on bag close content and open content
insert into mis_db.new_customer_tracking_event_mv
 SELECT --DISTINCT
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
 'DI')  ) and bcc.set_date < toDate('2025-08-18') and bcc.set_date >= toDate('2025-08-01')
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content AS boc ON (be.bag_number = boc.bag_number) AND (be.event_type IN ('OP',
 'OR')) and boc.set_date < toDate('2025-08-18') and boc.set_date >= toDate('2025-08-01')
LEFT JOIN mis_db.ext_mdm_office_master AS kom ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 ON be.to_office_id = kom2.office_id
AND be.transaction_date >= toDate('2025-08-01') and be.transaction_date < toDate('2025-08-18') and bcc.article_number is not null and boc.article_number is not null
-------------Query1 Booking date insertion 

---below worked
insert into mis_db.new_customer_tracking_event_mv
SELECT
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
INNER JOIN mis_db.ext_mdm_office_master AS kom
    ON kmd.md_office_id_bkg = kom.office_id
WHERE
    kmd.status_code = 'PC'
    AND kmd._peerdb_is_deleted = 0
    AND kmd.bulk_customer_id != 0
    AND kmd.md_updated_date >= toDate('2025-08-01')  -- ✅ date filter

---below memory error 
insert into mis_db.new_customer_tracking_event_mv
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
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0) AND (kmd.bulk_customer_id != 0)

---------very few records for the below....need to run all events query for the below 
INSERT INTO mis_db.amazon_target_table_dt (
        article_number, article_type, booking_date, booking_time,
        booking_office_facility_id, booking_office_name, booking_pin, sender_address_city,
        destination_office_facility_id, destination_office_name, destination_pincode, 
        destination_city, destination_country, receiver_name, invoice_no, line_item,
        weight_value, tariff, cod_amount, booking_type, contract_number, reference,
        bulk_customer_id, event_date, event_time, event_code, event_office_facilty_id,
        office_name, event_description, non_delivery_reason
    )
    SELECT DISTINCT
        cxcm.article_number, 
        cxcm.article_type,
        cxcm.booking_date,
    cxcm.booking_time,
        cxcm.booking_office_facility_id,
        cxcm.booking_office_name, 
        cxcm.booking_pin, 
        cxcm.sender_address_city, 
        cxcm.destination_office_facility_id,
        cxcm.destination_office_name, 
        cxcm.destination_pincode, 
        cxcm.destination_city, 
        cxcm.destination_country,
        cxcm.receiver_name, 
        cxcm.invoice_no, 
        cxcm.line_item, 
        cxcm.weight_value, 
        cxcm.tariff, 
        cxcm.cod_amount,
        cxcm.booking_type, 
        cxcm.contract_number, 
        cxcm.reference, 
        cxcm.bulk_customer_id,
      te.event_date AS event_date,
    te.event_date AS event_time,
        te.event_code,
        te.office_id AS event_office_facilty_id,
        te.office_name,
        te.event_type AS event_description,
        te.delivery_status AS non_delivery_reason
    FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
    INNER JOIN (
        SELECT 
            t1.article_number, 
            t1.event_date, 
            t1.event_type, 
            t1.event_code,
            t1.office_id, 
            t1.office_name, 
            t1.delivery_status
        FROM mis_db.new_customer_tracking_event_mv AS t1
        INNER JOIN (
            SELECT 
                article_number, 
                max(event_date) AS max_event_date
            FROM mis_db.new_customer_tracking_event_mv
            GROUP BY article_number
        ) AS t2 ON t1.article_number = t2.article_number 
                AND t1.event_date = t2.max_event_date
    ) te ON cxcm.article_number = te.article_number
    LEFT JOIN mis_db.customer_log AS el
        ON cxcm.bulk_customer_id = el.customer_id
    WHERE cxcm.bulk_customer_id != 0
        AND cxcm.booking_date < now()
        AND te.event_date = toDate('2025-08-18')-- :00:00)'
  AND te.event_date < toDateTime64(?, 6)
----below select only working 
INSERT INTO mis_db.new_customer_xml_facility_customer_new_mv

with addrss as (select address_id, pincode,city, addressee_name from mis_db.ext_mailbkg_booking_address final) ,
     ofcmaster as (select office_id, office_name, csi_facility_id from mis_db.dim_mdm_office final)

    SELECT DISTINCT     
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
    FROM (select booking_type_code, contract_id, bulk_customer_id, charged_weight, destination_pincode,
    		destination_office_name, origin_pincode,md_updated_date, article_number, mail_type_code,bkg_ref_id, 
    		sender_address_reference, sender_pincode, receiver_address_reference, receiver_pincode, charges_detail_id,
    		md_office_id_bkg, destination_office_id
    	  from mis_db.ext_mailbkg_mailbooking_dom final 
     where status_code = 'PC' and _peerdb_is_deleted =0 and 
     md_updated_date >= '2025-08-18 12:00:00' and md_updated_date < '2025-08-18 12:59:59') AS kmd 
    INNER JOIN addrss AS kba1 
    ON (kmd.sender_address_reference = kba1.address_id) AND (kmd.sender_pincode = kba1.pincode) 
    INNER JOIN addrss AS kba2 
    ON (kmd.receiver_address_reference = kba2.address_id) AND (kmd.receiver_pincode = kba2.pincode) 
    INNER JOIN 
    (select charges_detail_id, vp_cod_value,total_amount  from mis_db.ext_mailbkg_charges_detail final where
       md_cd_updated_date >= '2025-08-18 12:00:00' and md_cd_updated_date < '2025-08-18 12:59:59') AS kcd 
    ON kmd.charges_detail_id = kcd.charges_detail_id 
    INNER JOIN ofcmaster AS kom 
    ON kmd.md_office_id_bkg = kom.office_id 
    INNER JOIN ofcmaster AS kon 
    ON kmd.destination_office_id = kon.office_id order by booking_date desc
    
---------------------below select also not working
INSERT INTO mis_db.new_customer_xml_facility_customer_new_mv
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
INNER JOIN (
    SELECT address_id, pincode, city
    FROM mis_db.ext_mailbkg_booking_address
) AS kba1
    ON (kmd.sender_address_reference = kba1.address_id) AND (kmd.sender_pincode = kba1.pincode)
INNER JOIN (
    SELECT address_id, pincode, city, addressee_name
    FROM mis_db.ext_mailbkg_booking_address
) AS kba2
    ON (kmd.receiver_address_reference = kba2.address_id) AND (kmd.receiver_pincode = kba2.pincode)
INNER JOIN (
    SELECT charges_detail_id, total_amount, vp_cod_value
    FROM mis_db.ext_mailbkg_charges_detail
) AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN (
    SELECT office_id, csi_facility_id, office_name
    FROM mis_db.ext_mdm_office_master
) AS kom
    ON kmd.md_office_id_bkg = kom.office_id
INNER JOIN (
    SELECT office_id, csi_facility_id, office_name
    FROM mis_db.ext_mdm_office_master
) AS kon
    ON kmd.destination_office_id = kon.office_id
WHERE kmd.status_code = 'PC'
  AND kmd._peerdb_is_deleted = 0
  AND kmd.bulk_customer_id != 0
  AND kmd.md_updated_date >= '2025-08-19 00:00:00'
  AND kmd.md_updated_date <  '2025-08-19 08:00:00'

---below query memory error 
insert into mis_db.new_customer_xml_facility_customer_new_mv

SELECT DISTINCT
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
WHERE (kmd.status_code = 'PC') AND (kmd._peerdb_is_deleted = 0);

        
-------------------Query 2
DETACH TABLE mis_db.amazon_target_table_dt_ib_mv;
ATTACH TABLE mis_db.amazon_target_table_dt_ib_mv;

DROP TABLE mis_db.amazon_target_table_dt_ib_mv;

-----------query 1 
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

    toDateTime64(te.event_date,
 6) + toIntervalSecond(0) AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
INNER JOIN
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
WHERE cxcm.bulk_customer_id != 0
AND te.event_date >= '2025-08-19 00:00:00'
  AND te.event_date <  '2025-08-19 23:59:59'