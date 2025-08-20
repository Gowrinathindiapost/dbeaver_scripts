select * from mis_db.tracking_event_mv tem WHERE  source_table ='ext_mailbkg_mailbooking_dom''ext_pdmanagement_article_event''ext_bagmgmt_bag_event'
bagging last 2025-06-28 17:18:17
delivery last 2025-06-30 15:10:25
booking last 2025-06-30 13:39:29



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
	WHERE cxcm.bulk_customer_id = '1000002954'
		AND parseDateTimeBestEffort(cxcm.booking_date) < now()
		--AND (el.generation_time IS NULL OR toDateTime(te.event_date) > toDateTime(el.generation_time))
		--AND te.event_code='ITEM_BOOK'
		--AND te.event_date BETWEEN '2025-06-25 17:43:33.816612 AND '2025-06-25 17:43:33.816612'
		AND te.event_date BETWEEN parseDateTimeBestEffort('2025-05-25 00:00:00') 
                      AND parseDateTimeBestEffort('2025-06-25 23:59:59')
		AND (
    (CAST({eventCodeParam: String} AS String) = '' AND FALSE) -- Always FALSE if param is empty
)
select * --distinct(article_number) 
from mis_db.new_customer_xml_facility_customer_mv where bulk_customer_id='1000002954'
select * from mis_db.new_customer_tracking_event_mv AS te where te.event_date BETWEEN parseDateTimeBestEffort('2025-06-25 00:00:00') 
                      AND parseDateTimeBestEffort('2025-06-25 23:59:59')
                      
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
    WHERE cxcm.bulk_customer_id = '1000002954'  AND te.event_date BETWEEN '2025-06-24T11:28:51.198516Z' AND '2025-07-02T11:28:51.198516Z' AND te.event_code="ITEM_BOOK"
        AND parseDateTimeBestEffort(cxcm.booking_date) < now()
        
        
        
        ---------------
        
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
    formatDateTime((te.event_date), '%d%m%Y') AS event_date,
    formatDateTime((te.event_date), '%H%i%s') AS event_time,
    te.event_code AS event_code,
    te.office_id AS event_office_facilty_id, 
    te.office_name AS office_name, 
    te.event_type AS event_description,
    te.delivery_status as non_delivery_reason
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
    ANY INNER JOIN (
        SELECT 
            article_number, 
            max(event_date) AS max_event_date
        FROM mis_db.new_customer_tracking_event_mv
        GROUP BY article_number
    ) AS t2
    ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
) te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id = '1000002954'  
    AND te.event_date BETWEEN parseDateTimeBestEffort('2025-06-24T11:28:51.198516Z') 
                          AND parseDateTimeBestEffort('2025-07-02T11:28:51.198516Z')
    AND te.event_code = 'ITEM_BOOK'
    AND parseDateTimeBestEffort(cxcm.booking_date) < now()
    AND (el.generation_time IS NULL OR toDateTime(te.event_date) > toDateTime(el.generation_time))
    
    
    
    SELECT
    distinct(cxcm.article_number), 
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
    formatDateTime((te.event_date), '%d%m%Y') AS event_date,
    formatDateTime((te.event_date), '%H%i%s') AS event_time,
    te.event_code AS event_code,
    te.office_id AS event_office_facilty_id, 
    te.office_name AS office_name, 
    te.event_type AS event_description,
    te.delivery_status as non_delivery_reason
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
    ANY INNER JOIN (
        SELECT 
            article_number, 
            max(event_date) AS max_event_date
        FROM mis_db.new_customer_tracking_event_mv
        GROUP BY article_number
    ) AS t2
    ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date
) te ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id = '1000002954'
   -- AND parseDateTimeBestEffort(cxcm.booking_date) < now()
    AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-06-29T11:45:51.198516Z') 
    AND parseDateTimeBestEffort('2025-06-30T11:45:51.198516Z')
    --AND te.event_date = parseDateTimeBestEffort('2025-07-02T11:45:51.198516Z')
    --AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-02T00:45:51.198516Z') 
    --AND parseDateTimeBestEffort('2025-07-02T23:59:51.198516Z')
   -- AND (el.generation_time IS NULL OR toDateTime(te.event_date) > toDateTime(el.generation_time))
 AND te.event_code = 'ITEM_BOOK'
 
 select * from mis_db.ext_mailbkg_mailbooking_dom emmd WHERE  emmd.bulk_customer_id = '1000002954'
 AND emmd.md_updated_date  BETWEEN parseDateTimeBestEffort('2025-07-02T00:45:51.198516Z') 
    AND parseDateTimeBestEffort('2025-07-02T23:59:51.198516Z') and status_code='PC'
    
    
    select distinct(article_number) from mis_db.mv_new_customer_xml_facility_customer_mv emmd WHERE  emmd.bulk_customer_id = '1000002954'
 AND emmd.booking_date  BETWEEN parseDateTimeBestEffort('2025-07-02T00:45:51.198516Z') 
    AND parseDateTimeBestEffort('2025-07-02T23:59:51.198516Z')
    
    select * from mis_db.new_customer_tracking_event_mv emmd WHERE  article_number in (select distinct(article_number) from mis_db.mv_new_customer_xml_facility_customer_mv emmd WHERE  emmd.bulk_customer_id = '1000002954'
 AND emmd.booking_date  BETWEEN parseDateTimeBestEffort('2025-07-02T00:45:51.198516Z') 
    AND parseDateTimeBestEffort('2025-07-02T23:59:51.198516Z')) and event_code='ITEM_BOOK'
    
    
    select * from mis_db.new_customer_tracking_event_mv emmd WHERE  article_number in (select distinct(article_number) from mis_db.mv_new_customer_xml_facility_customer_mv emmd WHERE  emmd.bulk_customer_id = '1000002954'
 AND emmd.booking_date  BETWEEN parseDateTimeBestEffort('2025-07-02T00:45:51.198516Z') 
    AND parseDateTimeBestEffort('2025-07-02T23:59:51.198516Z')) and event_code='ITEM_BOOK'
    
    emmd.bulk_customer_id = '1000002954'
 AND emmd.event_date  BETWEEN parseDateTimeBestEffort('2025-07-02T00:45:51.198516Z') 
    AND parseDateTimeBestEffort('2025-07-02T23:59:51.198516Z')
    
    
    OPTIMIZE TABLE mis_db.new_customer_xml_facility_customer_mv FINAL;
    OPTIMIZE TABLE mis_db.new_customer_tracking_event_mv FINAL;
    SELECT * FROM system.parts WHERE table = 'new_customer_xml_facility_customer_mv';
SELECT * FROM system.columns WHERE table = 'new_customer_tracking_event_mv';

SELECT *
FROM mis_db.new_customer_tracking_event_mv
FINAL;

   explain analyze SELECT
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
WHERE cxcm.bulk_customer_id = 1000002954
    AND parseDateTimeBestEffort(cxcm.booking_date) < now()
    AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-16 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-16 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
    --AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN toDateTime64('2025-07-07 00:01:57.198516', 6) AND toDateTime64('2025-07-07 23:59:57.198516', 6) -- ClickHouse will handle the time.Time object conversion
    
    AND te.event_code = 'ITEM_BOOK'
    
    select * from mis_db.`ext_mailbkg_mailbooking_dom` e2 WHERE e2.`article_number` ='EZ771944890IN'
    e2.md_updated_date BETWEEN parseDateTimeBestEffort('2025-07-07 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-07 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
    and e2.bulk_customer_id = 1000002954
    
    select * from mis_db.`new_customer_tracking_event_mv` e2 where 
    e2.event_date BETWEEN parseDateTimeBestEffort('2025-07-07 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-07 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
    and e2.article_number in (select `article_number` from mis_db.`ext_mailbkg_mailbooking_dom`cxcm  where cxcm.bulk_customer_id = 1000002954
    AND cxcm.md_updated_date BETWEEN toDateTime64('2025-07-07 00:01:57.198516', 6) AND toDateTime64('2025-07-07 23:59:57.198516', 6))
    
    
    select * from mis_db.`mv_new_article_events_tracking_event` e2  where 
    e2.event_date BETWEEN parseDateTimeBestEffort('2025-07-07 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-07 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
    and e2.article_number in (select `article_number` from mis_db.`ext_mailbkg_mailbooking_dom`cxcm  where cxcm.bulk_customer_id = 1000002954
    AND cxcm.md_updated_date BETWEEN toDateTime64('2025-05-25 00:01:57.198516', 6) AND toDateTime64('2025-07-08 23:59:57.198516', 6))
    
    
    select * from mis_db.`new_customer_tracking_event_mv` e2  where 
    e2.event_date BETWEEN parseDateTimeBestEffort('2025-07-07 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-07 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
    and e2.article_number in (select `article_number` from mis_db.`ext_mailbkg_mailbooking_dom`cxcm  where cxcm.bulk_customer_id = 1000002954
    AND cxcm.md_updated_date BETWEEN toDateTime64('2025-05-25 00:01:57.198516', 6) AND toDateTime64('2025-07-08 23:59:57.198516', 6))
    
    SELECT n.* FROM `mis_db`.`new_customer_tracking_event_mv` AS n
WHERE toDateTime(`event_date`) = '2025-06-25 22:47:12.000'
    
	select * from mis_db.`mv_new_pdmanagement_article_transaction_tracking_event` e2  where 
    e2.event_date BETWEEN parseDateTimeBestEffort('2025-07-07 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-07 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
    and e2.article_number in (select `article_number` from mis_db.`ext_mailbkg_mailbooking_dom`cxcm  where cxcm.bulk_customer_id = 1000002954
    AND cxcm.md_updated_date BETWEEN toDateTime64('2025-05-25 00:01:57.198516', 6) AND toDateTime64('2025-07-08 23:59:57.198516', 6))
    
    
    select * from mis_db.`new_customer_xml_facility_customer_mv` e2  where 
    parseDateTimeBestEffort(e2.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-07 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-07 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
    and e2.article_number in (select `article_number` from mis_db.`ext_mailbkg_mailbooking_dom`cxcm  where cxcm.bulk_customer_id = 1000002954
    AND cxcm.md_updated_date BETWEEN toDateTime64('2025-05-25 00:01:57.198516', 6) AND toDateTime64('2025-07-08 23:59:57.198516', 6))

    select * from mis_db.`new_customer_xml_facility_customer_mv` e2  where 
    parseDateTimeBestEffort(e2.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-07 00:01:57.198516') AND parseDateTimeBestEffort('2025-07-07 23:59:57.198516') -- ClickHouse will handle the time.Time object conversion
    and e2.article_number in (select `article_number` from mis_db.`ext_mailbkg_mailbooking_dom`cxcm  where cxcm.bulk_customer_id = 1000002954
    AND cxcm.md_updated_date BETWEEN toDateTime64('2025-05-25 00:01:57.198516', 6) AND toDateTime64('2025-07-08 23:59:57.198516', 6))
    
    select * from mis_db.`new_customer_xml_facility_customer_mv` e2  INNER JOIN ( 
    SELECT t1.article_number, t1.event_date, t1.event_type, t1.event_code,t1.office_id, t1.office_name, t1.delivery_status
    FROM mis_db.new_customer_tracking_event_mv AS t1
    ANY INNER JOIN (
        SELECT article_number, max(event_date) AS max_event_date
        FROM mis_db.new_customer_tracking_event_mv
        GROUP BY article_number
    ) AS t2
    ON t1.article_number = t2.article_number AND t1.event_date = t2.max_event_date)
    
    SELECT
    e2.*,
    latest_events.event_date,
    latest_events.event_type,
    latest_events.event_code,
    latest_events.office_id,
    latest_events.office_name,
    latest_events.delivery_status
FROM
    mis_db.`new_customer_xml_facility_customer_mv` AS e2
INNER JOIN (
    SELECT
        t1_inner.article_number,
        t1_inner.event_date,
        t1_inner.event_type,
        t1_inner.event_code,
        t1_inner.office_id,
        t1_inner.office_name,
        t1_inner.delivery_status
    FROM
        mis_db.new_customer_tracking_event_mv AS t1_inner
    INNER JOIN (
        SELECT
            article_number,
            MAX(event_date) AS max_event_date
        FROM
            mis_db.new_customer_tracking_event_mv
        GROUP BY
            article_number
    ) AS t2
    ON t1_inner.article_number = t2.article_number
    AND t1_inner.event_date = t2.max_event_date
) AS latest_events -- This alias is now the source for event details
ON e2.article_number = latest_events.article_number;
     
    
    select * from mis_db.`ext_mailbkg_mailbooking_dom`cxcm 
    SELECT
    cxcm.article_number, cxcm.article_type,
    formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
    formatDateTime(parseDateTimeBestEffet(cxcm.booking_time), '%H%i%s') AS booking_time,
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
INNER JOIN mis_db.new_customer_tracking_event_mv te
    ON cxcm.article_number = te.article_number
LEFT JOIN mis_db.customer_log AS el
    ON cxcm.bulk_customer_id = el.customer_id
WHERE cxcm.bulk_customer_id = 1000002954
    -- This condition `parseDateTimeBestEffort(cxcm.booking_date) < now()` might be redundant or conflict with a specific date range.
    -- Consider if you need to keep it or replace it entirely with the BETWEEN clause.
    AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN toDateTime64('2025-07-02 00:53:57.198516') AND toDateTime64('2025-07-02 23:57:57.198516')
    AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-02 00:53:57.198516') AND parseDateTimeBestEffort('2025-07-02 23:57:57.198516')
    AND te.event_code = 'ITEM_BOOK'
    
    
    
    
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
WHERE cxcm.bulk_customer_id = 1000002954
    AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN parseDateTimeBestEffort('2025-07-02 00:53:57.198516') AND parseDateTimeBestEffort('2025-07-02 23:57:57.198516')
    AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-02 00:53:57.198516') AND parseDateTimeBestEffort('2025-07-02 23:57:57.198516')
    AND te.event_code = 'ITEM_BOOK'

    
    
    -- mis_db.customer_log definition

CREATE TABLE mis_db.customer_log  ON CLUSTER cluster_1S_2R
(

    `id` UUID DEFAULT generateUUIDv4(),

    `customer_id` Int64,

    `file_name` String,

    `generation_time` DateTime,

    `status` String,

    `error_message` String,

    `event_date_filter` String,

    `event_code_filter` String,

    `generated_article_count` Int32
)
ENGINE = ReplicatedMergeTree
ORDER BY generation_time
SETTINGS index_granularity = 8192;


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
WHERE cxcm.bulk_customer_id = 1000002954
    AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN toDateTime64('2025-07-16 00:01:57.198516', 6) AND toDateTime64('2025-07-16 23:59:57.198516', 6)
    AND te.event_date BETWEEN toDateTime64('2025-07-16 00:01:57.198516', 6) AND toDateTime64('2025-07-16 23:59:57.198516', 6)
    AND te.event_code = 'ITEM_BOOK'
    
    
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
WHERE cxcm.bulk_customer_id = 1000002954
    AND parseDateTimeBestEffort(cxcm.booking_date) BETWEEN toDate('2025-06-25 00:53:57.198516', 6) AND toDate('2025-06-25 23:57:57.198516', 6)
    AND te.event_code = 'ITEM_BOOK'
    AND te.event_date BETWEEN toDate('2025-06-25 00:53:57.198516', 6) AND toDate('2025-06-25 23:57:57.198516', 6)
    
    ---------------------------------------
    
    
    
    
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
	WHERE cxcm.bulk_customer_id = 1000002954
	    AND parseDateTimeBestEffort(cxcm.booking_date) < now()    	    
	    AND te.event_date BETWEEN parseDateTimeBestEffort('2025-07-02 00:53:57.198516') AND parseDateTimeBestEffort('2025-07-02 23:57:57.198516')
	    AND te.event_code = ?

	    
	    
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
INNER JOIN mis_db.ext_mdm_office_master AS kom ON kae.current_office_id = kom.office_id;


--
SELECT
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
 'DI'))
LEFT JOIN mis_db.ext_bagmgmt_bag_open_content AS boc ON (be.bag_number = boc.bag_number) AND (be.event_type IN ('OP',
 'OR'))
LEFT JOIN mis_db.ext_mdm_office_master AS kom ON be.from_office_id = kom.office_id
LEFT JOIN mis_db.ext_mdm_office_master AS kom2 ON be.to_office_id = kom2.office_id
    