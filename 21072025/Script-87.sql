Check your goal: do you need:

latest known status only if it happened on '2025-07-07' → Query 1

or all statuses that happened on '2025-07-07' → Query 2

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
	    te.delivery_status as non_delivery_reason,
	    te.source_table
	FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
	INNER JOIN (
	    SELECT t1.article_number, t1.event_date, t1.event_type, t1.event_code,t1.office_id, t1.office_name, t1.delivery_status,t1.source_table
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
	WHERE cxcm.bulk_customer_id = 1000002954
	    AND parseDateTimeBestEffort(cxcm.booking_date) < now()
	    AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-07 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-07 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
	    and te.article_number='AW777451408IN'
	    
	    
	    select * from mis_db.`new_customer_tracking_event_mv` e2  where article_number='AW777481838IN'
	    select * from mis_db.new_customer_xml_facility_customer_mv where article_number='AW777481838IN'
    e2.event_date BETWEEN parseDateTimeBestEffort('2025-07-07 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-07 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
    and e2.article_number in (select `article_number`,cxcm.`md_updated_date`  from mis_db.`ext_mailbkg_mailbooking_dom`cxcm  where cxcm.bulk_customer_id = 1000002954
    AND cxcm.md_updated_date BETWEEN toDateTime64('2025-05-25 00:01:57.198516', 6) AND toDateTime64('2025-07-07 23:59:57.198516', 6))
    
    select * from mis_db.`new_customer_tracking_event_mv` e2  where article_number='EZ771944890IN'
    select * from mis_db.`new_customer_tracking_event_mv` e2  where toDate(event_date) = toDate('2025-07-07')
    
    EZ771941496IN
    EZ771946048IN
    AW777504131IN
    
    select * from `ext_mailbkg_mailbooking_dom` e2 where article_number in ('EZ771941496IN','EZ771946048IN','AW777504131IN')
    
    select * from `ext_mailbkg_mailbooking_dom` e2 where md_updated_date:date > 2025-07-05 10:26:08.816617000
    
    SELECT * 
FROM `ext_mailbkg_mailbooking_dom` e2
WHERE md_updated_date > parseDateTime64BestEffort('2025-07-07 10:26:08.816617000')

SELECT * 
FROM `ext_mailbkg_mailbooking_dom` e2
WHERE toDate(md_updated_date) = toDate('2025-07-07') and  bulk_customer_id = '1000002954'
AW777451408IN
AW777606665IN
AW777638840IN
AW777653820IN
AW777663929IN
AW777674113IN
AW777700034IN
AW777710487IN
AW805177664IN
EZ771944784IN
EZ771958956IN

select * from `ext_pdmanagement_article_event` e2 where article_number in ('AW777451408IN',
'AW777606665IN',
'AW777638840IN',
'AW777653820IN',
'AW777663929IN',
'AW777674113IN',
'AW777700034IN',
'AW777710487IN',
'AW805177664IN',
'EZ771944784IN',
'EZ771958956IN'
)
select * from `ext_pdmanagement_article_transaction` where article_number in ('AW777451408IN',
'AW777606665IN',
'AW777638840IN',
'AW777653820IN',
'AW777663929IN',
'AW777674113IN',
'AW777700034IN',
'AW777710487IN',
'AW805177664IN',
'EZ771944784IN',
'EZ771958956IN'
)
    
    
    select * from `mv_new_booking_dom_to_tracking_event` where article_number in ('AW777451408IN',
'AW777606665IN',
'AW777638840IN',
'AW777653820IN',
'AW777663929IN',
'AW777674113IN',
'AW777700034IN',
'AW777710487IN',
'AW805177664IN',
'EZ771944784IN',
'EZ771958956IN'
)

 select * from `mv_new_customer_xml_facility_customer_mv` where article_number in ('AW777451408IN',
'AW777606665IN',
'AW777638840IN',
'AW777653820IN',
'AW777663929IN',
'AW777674113IN',
'AW777700034IN',
'AW777710487IN',
'AW805177664IN',
'EZ771944784IN',
'EZ771958956IN'
)

select * from `new_customer_xml_facility_customer_mv` where article_number in ('AW777451408IN',
'AW777606665IN',
'AW777638840IN',
'AW777653820IN',
'AW777663929IN',
'AW777674113IN',
'AW777700034IN',
'AW777710487IN',
'AW805177664IN',
'EZ771944784IN',
'EZ771958956IN'
)

select * from `new_customer_tracking_event_mv` where article_number in ('AW777451408IN',
'AW777606665IN',
'AW777638840IN',
'AW777653820IN',
'AW777663929IN',
'AW777674113IN',
'AW777700034IN',
'AW777710487IN',
'AW805177664IN',
'EZ771944784IN',
'EZ771958956IN'
)



SELECT
	    distinct(cxcm.article_number), cxcm.article_type,
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
	    te.delivery_status as non_delivery_reason,
	    te.source_table
	FROM mis_db.new_customer_xml_facility_customer_mv AS cxcm
	Inner join mis_db.new_customer_tracking_event_mv te ON cxcm.article_number = te.article_number
	LEFT JOIN mis_db.customer_log AS el
	    ON cxcm.bulk_customer_id = el.customer_id
	WHERE cxcm.bulk_customer_id = 1000002954
	    AND parseDateTimeBestEffort(cxcm.booking_date) < now()
	    AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-07 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-07 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
	    and te.article_number='AW777451408IN'
	    
	    
	    Check your goal: do you need:

latest known status only if it happened on '2025-07-07' → Query 1

or all statuses that happened on '2025-07-07' → Query 2
