SELECT
    article_number, booking_date, -- Select relevant columns to verify
    parseDateTimeBestEffort(booking_date) AS parsed_booking_date
FROM mis_db.customer_xml_customer_mv
WHERE bulk_customer_id = 1000002954
  AND parseDateTimeBestEffort(booking_date) > parseDateTimeBestEffort('2025-06-20T11:40:11.198516Z')
LIMIT 10; -- Add a limit for testing

select * from mis_db.customer_xml_customer_mv where article_number='EK525406515IN'
select * from mis_db.tracking_event_mv where article_number='EK525406515IN'
INSERT INTO mis_db.customer_xml_customer_mv (article_number, article_type, booking_date, booking_time, booking_office_facility_id, booking_office_name, booking_pin, sender_address_city, destination_office_facility_id, destination_office_name, destination_pincode, destination_city, destination_country, receiver_name, invoice_no, line_item, weight_value, tariff, cod_amount, booking_type, contract_number, reference, bulk_customer_id) 
VALUES('EK525406515IN', 'SP_INLAND', '2025-06-24 22:23:51.791618', '2025-06-24 22:23:51.791618', 21460007, 'Mysuru NSH', 570001, 'MYSURU', 22660999, 'Elathur SO Kozhikode', 673303, 'MYSURU', 'INDIA', 'SHAMA MYS', '2146000724062567614', '360.000', 60.00, 0, 0, 'WIC', 0, '', 1000002954);

EK525406589IN	SP_INLAND	2025-06-24 22:23:51.791618	2025-06-24 22:23:51.791618	21460007	Mysuru NSH	570001	MYSURU	22660999	Elathur SO Kozhikode	673303	MYSURU	INDIA	SHAMA MYS	2146000724062567614		360.000	60.00	0.00	WIC	0		1000002954

INSERT INTO mis_db.tracking_event_mv (article_number, event_date, event_type, office_id, office_name, source_table, delivery_status, sort_order) 
VALUES('EK525406515IN', '2025-05-28 23:53:30', 'Item Bagged', 29460002, 'Chennai NSH', 'ext_bagmgmt_bag_event', 'testing', 0);
RT293241228IN	2025-05-28 23:52:30	Item Bagged	29460002	Chennai NSH	ext_bagmgmt_bag_event		3

"event_code": null,
        "event_description": null,
        "event_office_facilty_id": null,
        "event_office_name": null,
        "event_date": null,
        "event_time": null
       select * from  mis_db.tracking_event_mv WHERE article_number = 'EK525406589IN'
       select * from  mis_db.customer_xml_customer_mv WHERE article_number = 'EK525406589IN'
SELECT 
toDateTime(event_date) AS event_date,			
toDateTime(event_date) AS event_time,
			event_type as event_code,
			office_id as event_office_facilty_id, 
			office_name as office_name, 
			 
			delivery_status as event_description
		FROM mis_db.tracking_event_mv
		WHERE article_number = 'EK525406589IN'
		ORDER BY event_date DESC, event_time DESC 
		LIMIT 1
		
		CREATE TABLE IF NOT EXISTS mis_db.tracking_event_mv ON CLUSTER cluster_1S_2R
(
    article_number String,
    event_date DateTime,
    event_type String,
    office_id Int32,    
    office_name String,
    source_table String,
    delivery_status String,
    sort_order UInt8
)
ENGINE = ReplicatedMergeTree
ORDER BY (article_number, event_date)
SETTINGS index_granularity = 8192;
		
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
drop table mis_db.customer_log 
ON CLUSTER cluster_1S_2R
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




SELECT
	  cb.article_number,
	  cb.booking_date,
	  cb.booking_office_facility_id,
	  cb.tariff,
	  cb.destination_office_name,
	  te.event_date,
	  te.event_type,
	  te.office_name
	FROM mis_db.customer_xml_customer_mv AS cb
	INNER JOIN (
	    SELECT article_number, max(event_date) AS latest_event_date
	    FROM mis_db.tracking_event_mv
	    GROUP BY article_number
	) latest_evt ON cb.article_number = latest_evt.article_number
	INNER JOIN mis_db.tracking_event_mv AS te
	  ON te.article_number = latest_evt.article_number AND te.event_date = latest_evt.latest_event_date
	LEFT JOIN mis_db.customer_log AS el
	ON cb.bulk_customer_id = el.customer_id
	WHERE el.generation_time IS NULL OR toDateTime(latest_evt.latest_event_date) > toDateTime(el.generation_time)
	
	
	
	
	SELECT
		article_number, article_type, booking_date, booking_time, booking_office_facility_id,
		booking_office_name, booking_pin, sender_address_city, destination_office_facility_id,
		destination_office_name, destination_pincode, destination_city, destination_country,
		receiver_name, invoice_no, line_item, weight_value, tariff, cod_amount,
		booking_type, contract_number, reference, bulk_customer_id
	FROM mis_db.customer_xml_customer_mv
	WHERE bulk_customer_id = '1000002954' AND parseDateTimeBestEffort(booking_date) < Now() 
	
	INNER JOIN (
	    SELECT article_number, max(event_date) AS latest_event_date
	    FROM mis_db.tracking_event_mv
	    GROUP BY article_number
	) latest_evt ON cb.article_number = latest_evt.article_number
	INNER JOIN mis_db.tracking_event_mv AS te
	  ON te.article_number = latest_evt.article_number AND te.event_date = latest_evt.latest_event_date
	LEFT JOIN mis_db.customer_log AS el
	ON cb.bulk_customer_id = el.customer_id
	WHERE el.generation_time IS NULL OR toDateTime(latest_evt.latest_event_date) > toDateTime(el.generation_time)
	
	
	
	SELECT
    cb.article_number,
    cb.article_type,
    cb.booking_date,
    cb.booking_time,
    cb.booking_office_facility_id,
    cb.booking_office_name,
    cb.booking_pin,
    cb.sender_address_city,
    cb.destination_office_facility_id,
    cb.destination_office_name,
    cb.destination_pincode,
    cb.destination_city,
    cb.destination_country,
    cb.receiver_name,
    cb.invoice_no,
    cb.line_item,
    cb.weight_value,
    cb.tariff,
    cb.cod_amount,
    cb.booking_type,
    cb.contract_number,
    cb.reference,
    cb.bulk_customer_id,
    te.event_date,
    te.event_type,
    te.office_name
FROM mis_db.customer_xml_customer_mv AS cb
INNER JOIN (
    SELECT article_number, max(event_date) AS latest_event_date
    FROM mis_db.tracking_event_mv
    GROUP BY article_number
) latest_evt ON cb.article_number = latest_evt.article_number
INNER JOIN mis_db.tracking_event_mv AS te
    ON te.article_number = latest_evt.article_number 
    --AND te.event_date = latest_evt.latest_event_date
LEFT JOIN mis_db.customer_log AS el
    ON cb.bulk_customer_id = el.customer_id
WHERE cb.bulk_customer_id = '1000002954'
  AND parseDateTimeBestEffort(cb.booking_date) < now()
  AND (el.generation_time IS NULL 
       OR toDateTime(latest_evt.latest_event_date) > toDateTime(el.generation_time))
       
       SELECT parseDateTimeBestEffort('2025-06-21 11:41:58.140177') < now()
       SELECT *
FROM mis_db.tracking_event_mv
WHERE article_number = 'EK525406589IN'

SELECT
    cb.article_number,
    cb.booking_date,
    cb.bulk_customer_id
FROM mis_db.customer_xml_customer_mv AS cb
WHERE cb.bulk_customer_id = '1000002954'
  AND parseDateTimeBestEffort(left(cb.booking_date, 19)) < now() AND article_number = 'EK525406589IN'
  
  SELECT *
FROM mis_db.tracking_event_mv
WHERE article_number = 'EK525406589IN'
SELECT *
FROM mis_db.customer_log
WHERE customer_id = 1000002954

SELECT *
FROM mis_db.tracking_event_mv
WHERE article_number IN (
    SELECT article_number
    FROM mis_db.customer_xml_customer_mv
    WHERE bulk_customer_id = 1000002954
)



SELECT *
FROM mis_db.customer_xml_customer_mv
WHERE bulk_customer_id = 1000002954

SELECT max(event_date)
FROM mis_db.tracking_event_mv
WHERE article_number IN (
  SELECT article_number
  FROM mis_db.customer_xml_customer_mv
  WHERE bulk_customer_id = 1000002954
)

SELECT booking_date
FROM mis_db.customer_xml_customer_mv
WHERE bulk_customer_id = 1000002954
LIMIT 10

SELECT booking_date, parseDateTimeBestEffort(booking_date)
FROM mis_db.customer_xml_customer_mv
WHERE bulk_customer_id = 1000002954


SELECT
  cxcm.article_number, cxcm.article_type, cxcm.booking_date, cxcm.booking_time, cxcm.booking_office_facility_id,
  cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
  cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
  cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
  cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
  te.event_date AS event_date,
  te.event_date AS event_time,
  te.event_type as event_code,
  te.office_id as event_office_facilty_id,
  te.office_name as office_name,
  te.delivery_status as event_description
FROM mis_db.customer_xml_customer_mv AS cxcm
INNER JOIN (
    SELECT *
    FROM mis_db.tracking_event_mv AS t1
    ANY INNER JOIN (
        SELECT article_number, max(event_date) AS max_event_date
        FROM mis_db.tracking_event_mv
        GROUP BY article_number
    ) AS t2
    ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
) te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
  ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id = 1000002954
  AND parseDateTimeBestEffort(cxcm.booking_date) < now()
  AND (el.generation_time IS NULL OR toDateTime(te.event_date) > toDateTime(el.generation_time))---
  --------------------
  INSERT INTO mis_db.customer_xml_customer_mv
  
  select * from mis_db.tracking_event_mv tem 
  
  SELECT
    *,
    parseDateTime64BestEffort(replaceRegexpAll(event_date, '\\[.*?\\]', '')) AS event_date_formatted
FROM mis_db.tracking_event_mv

  EK873800882IN
  EK541970207IN
  EK873801018IN
  
  
  
  SELECT
		cxcm.article_number, cxcm.article_type, 
		--formatDateTime(toDateTime(cxcm.booking_date), '%d%m%Y'),
		--formatDateTime(toDateTime(cxcm.booking_time), '%H%M%S'),
		formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS formatted_booking_date,
		 formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%M%S') AS formatted_booking_time,
		cxcm.booking_office_facility_id,
		cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
		cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
		cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
		cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
		te.event_date AS event_date,
		te.event_date AS event_time,
		te.event_type AS event_code,
		te.office_id AS event_office_facilty_id, 
		te.office_name AS office_name, 
		te.delivery_status AS event_description
	FROM mis_db.customer_xml_customer_mv AS cxcm
	INNER JOIN (
		SELECT *
		FROM mis_db.tracking_event_mv AS t1
		ANY INNER JOIN (
			SELECT article_number, max(event_date) AS max_event_date
			FROM mis_db.tracking_event_mv
			GROUP BY article_number
		) AS t2
		ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
	) te ON cxcm.article_number = te.article_number
	LEFT JOIN mis_db.customer_log AS el
		ON cxcm.bulk_customer_id = el.customer_id
	WHERE cxcm.bulk_customer_id = '1000002954'
		AND parseDateTimeBestEffort(cxcm.booking_date) < now()
		AND (el.generation_time IS NULL OR toDateTime(te.event_date) > toDateTime(el.generation_time))
  