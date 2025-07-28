CREATE TABLE mis_db.customer_xml_customer_mv ON CLUSTER cluster_1S_2R
(
    `article_number` String,
    `article_type` String,
    `booking_date` String,  -- Changed from DateTime
    `booking_time` String,  -- Changed from DateTime
    `booking_office_facility_id` Int64, -- Use Int64 as per MV, or check source type
    `booking_office_name` String,
    `booking_pin` Int32,
    `sender_address_city` String,
    `destination_office_facility_id` Int32,
    `destination_office_name` String,
    `destination_pincode` Int32,
    `destination_city` String,
    `destination_country` String,
    `receiver_name` String,
    `invoice_no` String,
    `line_item` String,
    `weight_value` Decimal(10, 3),
    `tariff` Decimal(10, 2),
    `cod_amount` Decimal(10, 2), -- Changed from Float32
    `booking_type` String,
    `contract_number` Int32,  -- Changed from String
    `reference` String,
    `bulk_customer_id` Int64
    -- Event columns are still missing from your MV's SELECT.
    -- If you want them in the target table, you must add them to the MV's SELECT.
    -- Otherwise, remove them from the CREATE TABLE statement.
--    `event_code` String,
--    `event_description` String,
--    `event_office_facilty_id` Int32,
--    `event_office_name` String,
--    `event_date` DateTime,
--    `event_time` DateTime,
--    `non_del_reason` String
)
ENGINE = ReplicatedMergeTree
ORDER BY (article_number)
SETTINGS index_granularity = 8192;




CREATE MATERIALIZED VIEW mis_db.mv_customer_xml_customer_mv -- Renamed MV for clarity, or use your preferred MV name
ON CLUSTER cluster_1S_2R
TO mis_db.customer_xml_customer_mv -- <--- THIS MUST MATCH YOUR CREATE TABLE NAME
AS
INSERT INTO mis_db.customer_xml_customer_mv
SELECT
    kmd.article_number AS article_number,
    kmd.mail_type_code AS article_type,
    kmd.md_updated_date AS booking_date,
    kmd.md_updated_date AS booking_time,
    kmd.md_office_id_bkg AS booking_office_facility_id,
    kom.office_name AS booking_office_name,
    kmd.origin_pincode AS booking_pin,
    'MYSORE' AS sender_address_city,
    kmd.destination_office_id AS destination_office_facility_id,
    kmd.destination_office_name AS destination_office_name,
    kmd.destination_pincode AS destination_pincode,
   'CHENNAI' AS destination_city,
    'INDIA' AS destination_country,
    'CHENNAI' AS receiver_name,
   	kmd.bkg_ref_id AS invoice_no,
    '' AS line_item,
    kmd.charged_weight AS weight_value,
    kcd.total_amount AS tariff,
    kcd.vp_cod_value AS cod_amount,
    kmd.booking_type_code AS booking_type,
    kmd.contract_id AS contract_number,
    '' AS reference,
    kmd.bulk_customer_id AS bulk_customer_id
FROM mis_db.ext_bkg_mailbooking_dom AS kmd
--INNER JOIN mis_db.ext_bkg_booking_address AS kba1
--    ON kmd.sender_address_reference = kba1.address_id
--INNER JOIN mis_db.ext_bkg_booking_address AS kba2
--    ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_bkg_charges_detail AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom
    ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.status_code = 'PC' AND kmd._peerdb_is_deleted = 0;