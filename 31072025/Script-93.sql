select * from mis_db.`ext_mailbkg_mailbooking_dom` e2 where e2.article_number='PP576355033IN' 
select * from mis_db.new_customer_tracking_event_mv where  article_number='PP576355033IN'
select * from mis_db.ext_pdmanagement_article_event where article_number='PP576355033IN'
select * from mis_db.ext_bagmgmt_bag_event where bag_number in 
(select bag_number from mis_db.ext_bagmgmt_bag_close_content where article_number='PP576355033IN')
select * from mis_db.tracking_event_mv e2 where e2.article_number='PP576355033IN' 
select * from mis_db.tracking_event_mv e2 where e2.article_number='PP576355033IN'  'AW771505981IN' 'CK525226131IN'
select * from bagmgmt.bag_event  where e2.article_number='PP576355033IN'
select * from bagmgmt.bag_close_content where article_number='PP576355033IN'

select * from tracking_event_mv tem where source_table='ext_bagmgmt_bag_event'  and article_number='PP576355033IN' like '%IN'
SELECT 
			article_number,
			bookedat,
			bookedon,
			toString(destination_pincode),
			toFloat64(tariff),
			article_type,
			delivery_location
		FROM mis_db.mv_booking_dom
		WHERE article_number = 'PP576355033IN'
		
		SELECT 
			article_number,
			toDateTime(event_date) AS event_date,
			office_name AS event_office,
			event_type
		FROM mis_db.tracking_event_mv
		WHERE article_number = 'PP576355033IN'

select * from mis_db.mv_article_events_tracking_event e2 where e2.article_number='CK525226131IN' 

select * from mis_db.ext_pdmanagement_article_transaction e2 where e2.article_number='CK525226131IN'

select * from mis_db.ext_pdmanagement_article_event e2 where e2.article_number='CK525226131IN'

select * from mis_db.`ext_mailbkg_mailbooking_dom` e2 where e2.article_number='CK525226131IN'

select * from mis_db.ext_bagmgmt_bag_event where bag_number in 
(select bag_number from mis_db.ext_bagmgmt_bag_close_content where article_number='CK525226131IN')


SELECT
    article_number,

    now() AS event_date,

    multiIf(action_code = 1,
 'Item Delivery',
 'Not Delivered') AS event_type,

    multiIf(action_code = 1,
 'ITEM_DELIVERY',
 'ITEM_NONDELIVER') AS event_code,

    csi_facility_id AS office_id,

    current_office_name AS office_name,

    'ext_pdmanagement_article_transaction' AS source_table,

    multiIf(action_code = 1,
 '',
 epat.remarks) AS delivery_status,

    7 AS sort_order
FROM mis_db.ext_pdmanagement_article_transaction AS epat
INNER JOIN mis_db.ext_mdm_office_master AS kom ON epat.current_office_id = kom.office_id
WHERE article_number IS NOT NULL;


SELECT
    cxcm.article_number, cxcm.article_type,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
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
WHERE cxcm.bulk_customer_id = '1000002954'
AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-01 00:53:57.198516') AND parseDateTimeBestEffort('2025-07-14 23:59:57.198516')
AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-01 00:53:57.198516') AND parseDateTimeBestEffort('2025-07-13 23:59:57.198516')
  AND te.event_code = 'ITEM_BOOK'
select * from customer_log
select distinct(article_number),cxcm.* from new_customer_xml_facility_customer_mv cxcm
where parseDateTimeBestEffort(cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-13 00:53:57.198516') AND parseDateTimeBestEffort('2025-07-14 23:59:57.198516')
and  cxcm.bulk_customer_id = '1000002954'

SELECT DISTINCT  cxcm.*
FROM new_customer_xml_facility_customer_mv cxcm
WHERE parseDateTimeBestEffort(cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-13 00:53:57.198516')
                                                  AND parseDateTimeBestEffort('2025-07-14 23:59:57.198516')
  AND cxcm.bulk_customer_id = '1000002954';


SELECT DISTINCT  te.*,cxcm.*
FROM new_customer_tracking_event_mv te
WHERE te.event_date BETWEEN parseDateTimeBestEffort('2025-07-13 00:53:57.198516')
                                                  AND parseDateTimeBestEffort('2025-07-14 23:59:57.198516')
  AND te.event_code = 'ITEM_BOOK' and `article_number` in (
  SELECT DISTINCT  cxcm.`article_number` 
FROM new_customer_xml_facility_customer_mv cxcm
WHERE parseDateTimeBestEffort(cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-13 00:53:57.198516')
                                                  AND parseDateTimeBestEffort('2025-07-14 23:59:57.198516')
  AND cxcm.bulk_customer_id = '1000002954'
  )

  
  SELECT DISTINCT te.*, cxcm.*
FROM new_customer_tracking_event_mv te
JOIN new_customer_xml_facility_customer_mv cxcm
  ON te.article_number = cxcm.article_number
WHERE parseDateTimeBestEffort(te.event_date) BETWEEN parseDateTimeBestEffort('2025-07-13 00:53:57.198516')
                                               AND parseDateTimeBestEffort('2025-07-14 23:59:57.198516')
  AND te.event_code = 'ITEM_BOOK'
  AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-13 00:53:57.198516')
                                                   AND parseDateTimeBestEffort('2025-07-14 23:59:57.198516')
  AND cxcm.bulk_customer_id = '1000002954';

SELECT DISTINCT te.*, cxcm.*
FROM new_customer_tracking_event_mv AS te
INNER JOIN new_customer_xml_facility_customer_mv AS cxcm
    ON te.article_number = cxcm.article_number
WHERE te.event_date BETWEEN parseDateTimeBestEffort('2025-07-01 00:53:57.198511') AND parseDateTimeBestEffort('2025-07-13 23:59:57.198512')
  AND te.event_code = 'ITEM_BOOK'
  AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-01 00:53:57.198513') AND parseDateTimeBestEffort('2025-07-14 23:59:57.198514')
  AND cxcm.bulk_customer_id = '1000002954'

SELECT
    cxcm.article_number, cxcm.article_type,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
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
WHERE cxcm.bulk_customer_id = '1000002954'
AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN '2025-07-13 00:00:00' AND '2025-07-14 23:59:59'
AND te.event_date BETWEEN '2025-07-13 00:00:00' AND '2025-07-13 23:59:59'
AND te.event_code = 'ITEM_BOOK'

----dev query from logs
SELECT
    cxcm.article_number, 
    cxcm.article_type,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
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
WHERE cxcm.bulk_customer_id = '1000002954'
  AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-01 00:00:59.198511') AND parseDateTimeBestEffort('2025-07-14 23:59:59.198512')
  AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-01 00:00:59.198513') AND parseDateTimeBestEffort('2025-07-14 23:59:59.198514')
  AND te.event_code = 'ITEM_BOOK'


