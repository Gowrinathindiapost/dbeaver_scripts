-- mis_db.amazon_target_table_dt_mv source

select * from mis_db.tracking_event_mv where article_number='TA443828461IN'
select * from mis_db.ext_pdmanagement_article_transaction  where article_number='TA443828461IN'
select * from mis_db.ext_pdmanagement_article_event  where article_number='TA443828461IN'
optimize TABLE mis_db.new_customer_tracking_event_new_mv FINAL ON CLUSTER cluster_1S_2R;
optimize TABLE mis_db.new_customer_xml_facility_customer_new_mv FINAL ON CLUSTER cluster_1S_2R;
select * from mis_db.amazon_target_table_dt where bulk_customer_id='1000002954' article_number='TA443828461IN' 

OPTIMIZE TABLE mis_db.amazon_target_table_dt FINAL;

CREATE MATERIALIZED VIEW mis_db.amazon_target_table_dt_mv TO mis_db.amazon_target_table_dt
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
AS SELECT
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

    te.event_date AS event_time,

    te.event_code AS event_code,

    te.office_id AS event_office_facilty_id,

    te.office_name AS office_name,

    te.event_type AS event_description,

    te.delivery_status AS non_delivery_reason
FROM mis_db.new_customer_xml_facility_customer_new_mv AS cxcm
INNER JOIN
(
    SELECT
        t1.article_number,

        t1.event_date,

        t1.event_type,

        t1.event_code,

        t1.office_id,

        t1.office_name,

        t1.delivery_status,

        row_number() OVER (PARTITION BY t1.article_number ORDER BY t1.event_date DESC) AS rn
    FROM mis_db.new_customer_tracking_event_new_mv AS t1
) AS te ON (cxcm.article_number = te.article_number) AND (te.rn = 1)
LEFT JOIN mis_db.customer_log AS el ON cxcm.bulk_customer_id = el.customer_id
WHERE (cxcm.bulk_customer_id != 0) AND (cxcm.booking_date < now());


SELECT
	    cxcm.article_number, cxcm.article_type,
	    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
	    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
	    cxcm.booking_office_facility_id,
	    cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
	    cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
	    cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
	    cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
	    formatDateTime((te.event_date), '%d%m%Y') AS event_date,
	    formatDateTime((te.event_date), '%H%i%s') AS event_time,
	    te.event_code AS event_code,
	    te.office_id AS event_office_facilty_id,
	    te.office_name AS office_name,
	    te.event_type AS event_description,
	    te.delivery_status as non_delivery_reason
	FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
	INNER JOIN (
	    SELECT t1.article_number, t1.event_date, t1.event_type, t1.event_code,t1.office_id, t1.office_name, t1.delivery_status
	    FROM mis_db.new_customer_tracking_event_mv AS t1
	    ANY INNER JOIN (
	        SELECT article_number, max(event_date) AS max_event_date
	        FROM mis_db.new_customer_tracking_event_mv
	        GROUP BY article_number
	    ) AS t2
	    ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
	) te ON cxcm.article_number = te.article_number
	LEFT JOIN mis_db.customer_log AS el
	    ON cxcm.bulk_customer_id = el.customer_id
	WHERE --cxcm.bulk_customer_id = ?
	    --AND 
	    parseDateTimeBestEffort(cxcm.booking_date) < now()
	    
	    SELECT 
    cxcm.*,
    te.event_date,
    te.event_type,
    te.event_code,
    te.office_id,
    te.office_name,
    te.delivery_status
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
) AS te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE parseDateTimeBestEffort(cxcm.booking_date) < now()
    AND cxcm.bulk_customer_id = '1000002954'
     AND te.event_date >= parseDateTime64BestEffort('2025-07-24 00:01:57.198516')
  AND te.event_date < parseDateTime64BestEffort('2025-07-25 00:01:57.198516')
ORDER BY te.event_date DESC



INSERT INTO mis_db.new_customer_tracking_event_new_mv (article_number, event_date, event_type, event_code, 
office_id, office_name, source_table, delivery_status, sort_order) 
VALUES('AY619155955IN', '2025-07-25 20:22:44', 'Not Delivered', 'ITEM_NONDELIVER', 
'PO23310209000', 'Gwalior Residency S.O', 'ext_pdmanagement_article_transaction', 'Door Locked', 7);


SELECT
    name AS materialized_view_name,
    engine,
    create_table_query
FROM system.tables
WHERE engine = 'MaterializedView'
  AND database = 'mis_db'
  AND name = 'fct_crm_raw_bulk_last_event_mv'
  AND create_table_query ILIKE '%ReplicatedReplacingMergeTree%'
  
  
  SELECT
    *
FROM system.dependencies
WHERE
    database = 'mis_db'
    AND table = 'fct_crm_raw_bulk_last_event'

    SELECT 
    name AS view_name,
    create_table_query
FROM system.tables
WHERE engine = 'MaterializedView'
AND database = 'mis_db'
AND create_table_query LIKE '%TO mis_db.fct_crm_raw_bulk_last_event%';






