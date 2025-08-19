mv_mailbooking_dom_customer_xml_PC_test_mv
CREATE TABLE mis_db.mailbooking_dom_xml_customer_PC_mv ON CLUSTER cluster_1S_2R
(
    article_number String,
    article_type String,
    booking_date String,
    booking_time String,
    booking_office_facility_id Int32,
    booking_office_name String,
    booking_pin Int32,
    sender_address_city String,
    destination_office_facility_id Int32,
    destination_office_name String,
    destination_pin Int32,
    destination_city String,
    destination_country String,
    receiver_name String,
    invoice_no String,
    line_item String,
    weight_value String,
    tariff Decimal(10, 2),
    cod_amount Float32,
    booking_type String,
    contract_number String,
    reference String,
    --event_code String,
    --event_description String,
--    event_office_facilty_id Int32,
--    event_office_name String,
--    event_date DateTime,
--    event_time DateTime,
--    non_del_reason String
    
    )
ENGINE = ReplicatedMergeTree
ORDER BY (article_number)
SETTINGS index_granularity = 8192;

RK457554674IN	LETTER	18062025	140107
select * from mis_db.ext_mailbkg_mailbooking_dom where article_number='AW777093113IN''EK093727385IN''RK457554674IN'
select * from mis_db.mv_mailbooking_dom_customer_xml_mv where article_number='EK093727385IN'
select * from mis_db.ext_mailbkg_charges_detail where charges_detail_id='831582960328496''273136632102912'

CREATE MATERIALIZED VIEW mis_db.mv_mailbooking_dom_customer_xml_PC_test_mv
ON CLUSTER cluster_1S_2R
--ORDER
TO mis_db.mailbooking_dom_customer_xml_PC_test_mv
--ENGINE = ReplacingMergeTree()
--ORDER BY article_number
AS

SELECT
    kmd.article_number AS article_number,
    kmd.mail_type_code AS article_type,
    formatDateTime(kmd.md_created_date, '%d%m%Y') AS booking_date,
    formatDateTime(kmd.md_created_date, '%H%i%s') AS booking_time,
    kmd.md_office_id_bkg AS booking_office_facility_id,
    kom.office_name AS booking_office_name,
    kmd.origin_pincode AS booking_pin,
    kba1.city AS sender_address_city,
    kmd.destination_office_id AS destination_office_facility_id,
    kmd.destination_office_name AS destination_office_name,
    kmd.destination_pincode AS destination_pin,
    kba2.city AS destination_city,
    'INDIA' AS destination_country,
    kba2.addressee_name AS receiver_name,
   	kmd.bkg_ref_id AS invoice_no,
    '' AS line_item,
    kmd.charged_weight AS weight_value,
    kcd.total_amount AS tariff,
    kcd.vp_cod_value AS cod_amount,--vp_cod_value
    kmd.booking_type_code AS booking_type,--booking_type_code--RBC register bulk customer --wic walk in customer --mail_form_code--C:Parcel,E:EMS
    kmd.contract_id AS contract_number,--contractor_id
    '' AS reference--pg_trans_ref
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1 
    ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2 
    ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd 
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom 
    ON kmd.md_office_id_bkg = kom.office_id
    WHERE kmd.status_code = 'PC';
WHERE kmd._peerdb_is_deleted = 0;

select count(*) from mis_db.ext_mailbkg_mailbooking_dom where bulk_customer_id='2954' '6000001799'
SELECT count(*)
FROM mis_db.ext_mailbkg_mailbooking_dom
WHERE bulk_customer_id='1000002954';

SELECT *
FROM mis_db.ext_mailbkg_mailbooking_dom
WHERE toString(bulk_customer_id) LIKE '%295%';

select * from mis_db.mailbooking_dom_customer_xml_PC_mv


CREATE MATERIALIZED VIEW mis_db.mv_mailbooking_dom_xml_customer_PC_mv
ON CLUSTER cluster_1S_2R
--ORDER
TO mis_db.mailbooking_dom_xml_customer_PC_mv
--ENGINE = ReplacingMergeTree()
--ORDER BY article_number
AS
--INSERT INTO mis_db.mailbooking_dom_customer_xml_PC_mv
SELECT
    kmd.article_number AS article_number,
    kmd.mail_type_code AS article_type,
    formatDateTime(kmd.md_created_date, '%d%m%Y') AS booking_date,
    formatDateTime(kmd.md_created_date, '%H%i%s') AS booking_time,
    kmd.md_office_id_bkg AS booking_office_facility_id,
    kom.office_name AS booking_office_name,
    kmd.origin_pincode AS booking_pin,
    kba1.city AS sender_address_city,
    kmd.destination_office_id AS destination_office_facility_id,
    kmd.destination_office_name AS destination_office_name,
    kmd.destination_pincode AS destination_pin,
    kba2.city AS destination_city,
    'INDIA' AS destination_country,
    kba2.addressee_name AS receiver_name,
   	kmd.bkg_ref_id AS invoice_no,
    '' AS line_item,
    kmd.charged_weight AS weight_value,
    kcd.total_amount AS tariff,
    kcd.vp_cod_value AS cod_amount,--vp_cod_value
    kmd.booking_type_code AS booking_type,--booking_type_code--RBC register bulk customer --wic walk in customer --mail_form_code--C:Parcel,E:EMS
    kmd.contract_id AS contract_number,--contractor_id
    '' AS reference--pg_trans_ref
FROM mis_db.ext_mailbkg_mailbooking_dom AS kmd
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba1
    ON kmd.sender_address_reference = kba1.address_id
INNER JOIN mis_db.ext_mailbkg_booking_address AS kba2
    ON kmd.receiver_address_reference = kba2.address_id
INNER JOIN mis_db.ext_mailbkg_charges_detail AS kcd
    ON kmd.charges_detail_id = kcd.charges_detail_id
INNER JOIN mis_db.ext_mdm_office_master AS kom
    ON kmd.md_office_id_bkg = kom.office_id
WHERE kmd.status_code = 'PC' AND kmd._peerdb_is_deleted = 0;



SELECT
    t2.*
FROM
    mis_db.xml_customer_mv AS t2
WHERE
    t2.article_number IN (
        SELECT
            t1.article_number
        FROM
            mis_db.ext_mailbkg_mailbooking_dom AS t1
        WHERE
            toString(t1.bulk_customer_id) LIKE '%295%'
    );

select * from mis_db.xml_customer_mv xcm  where booking_date <> LIKE '1970%-10-06 17:23:45'
SELECT *
FROM mis_db.mailbooking_dom_customer_xml_PC_mv
WHERE toString(booking_date) NOT LIKE '1970%';


CREATE TABLE mis_db.customer_log 
ON CLUSTER cluster_1S_2R
(
    id Int64,
    customer_id Int64,
    file_name String,
    generation_time DateTime,
    status String,
    error_message String,
    event_date_filter String,
    event_code_filter String,
    generated_article_count Int32
)
ENGINE = ReplicatedMergeTree
ORDER BY (generation_time)
SETTINGS index_granularity = 8192;


CREATE TABLE mis_db.customer_log 
ON CLUSTER cluster_1S_2R
(
    id UUID DEFAULT generateUUIDv4(),
    customer_id Int64,
    file_name String,
    generation_time DateTime,
    status String,
    error_message String,
    event_date_filter String,
    event_code_filter String,
    generated_article_count Int32
)
ENGINE = ReplicatedMergeTree
ORDER BY (generation_time)
SETTINGS index_granularity = 8192;

