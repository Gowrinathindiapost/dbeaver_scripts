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
	    AND te.event_date BETWEEN 
	    parseDateTimeBestEffort('2025-07-16 00:53:57.198516') AND 
        parseDateTimeBestEffort('2025-07-16 23:57:57.198516')
        
        
        
        -- Composite index for bulk_customer_id and booking_date filters
ALTER TABLE mis_db.new_customer_xml_facility_customer_mv
ADD INDEX idx_cxcm_customer_date (bulk_customer_id, parseDateTimeBestEffort(booking_date))
TYPE minmax GRANULARITY 3;

-- Index for article_number used in JOIN
ALTER TABLE mis_db.new_customer_xml_facility_customer_mv
ADD INDEX idx_cxcm_article (article_number) TYPE bloom_filter GRANULARITY 4;

-- Composite index for finding latest events per article
ALTER TABLE mis_db.new_customer_tracking_event_mv
ADD INDEX idx_te_article_date (article_number, event_date)
TYPE minmax GRANULARITY 3;

-- Additional index for event_date range queries
ALTER TABLE mis_db.new_customer_tracking_event_mv
ADD INDEX idx_te_date (event_date) TYPE minmax GRANULARITY 3;

ALTER TABLE mis_db.new_customer_xml_facility_customer_mv
ADD COLUMN booking_date_parsed DateTime MATERIALIZED parseDateTimeBestEffort(booking_date);

ALTER TABLE mis_db.new_customer_tracking_event_mv
ADD COLUMN event_date_parsed DateTime MATERIALIZED parseDateTimeBestEffort(event_date);