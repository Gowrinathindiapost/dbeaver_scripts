SELECT
		cxcm.article_number, cxcm.article_type, 
		formatDateTime(parseDateTimeBestEffort(cxcm.booking_date), '%d%m%Y') AS booking_date,
		 formatDateTime(parseDateTimeBestEffort(cxcm.booking_time), '%H%i%s') AS booking_time,
		cxcm.booking_office_facility_id,
		cxcm.booking_office_name, cxcm.booking_pin, cxcm.sender_address_city, cxcm.destination_office_facility_id,
		cxcm.destination_office_name, cxcm.destination_pincode, cxcm.destination_city, cxcm.destination_country,
		cxcm.receiver_name, cxcm.invoice_no, cxcm.line_item, cxcm.weight_value, cxcm.tariff, cxcm.cod_amount,
		cxcm.booking_type, cxcm.contract_number, cxcm.reference, cxcm.bulk_customer_id,
--		formatDateTime((te.event_date), '%d%m%Y') AS event_date,
--		 formatDateTime((te.event_date), '%H%i%s') AS event_time,
--		 te.event_code AS event_code,
--		te.office_id AS event_office_facilty_id, 
--		te.office_name AS office_name, 
--		te.event_type AS event_description,
--		te.delivery_status as non_delivery_reason
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
		AND (el.generation_time IS NULL OR toDateTime(te.event_date) > toDateTime(el.generation_time))
		select * from mis_db.customer_xml_customer_mv AS cxcm
		select * from mis_db.new_customer_xml_facility_customer_mv where article_number='CK012529815IN'
		SELECT count() FROM mis_db.new_customer_xml_facility_customer_mv;
select * from mis_db.new_customer_xml_facility_customer_mv where bulk_customer_id = '1000002954' article_number in (select article_number from mis_db.new_customer_tracking_event_mv where article_number <>'' )
SELECT booking_date, booking_time FROM mis_db.new_customer_xml_facility_customer_mv LIMIT 10;
SELECT count() FROM mis_db.new_customer_xml_facility_customer_mv WHERE tariff IS NULL;
SELECT count() FROM mis_db.new_customer_tracking_event_mv;
SELECT DISTINCT bulk_customer_id FROM mis_db.new_customer_xml_facility_customer_mv LIMIT 10;--CK012529815IN
select * from mis_db.new_customer_tracking_event_mv where article_number='' 'CK012529815IN'

ALTER TABLE mis_db.new_customer_tracking_event_mv 
DELETE WHERE (article_number = '' OR article_number IS NULL);


INSERT INTO mis_db.new_customer_tracking_event_mv (article_number, event_date, event_type, event_code, office_id, office_name, source_table, delivery_status, sort_order) 
VALUES('CK012529815IN', '2025-06-25 16:24:44.186071', 'ITEM BOOK', 'ITEM_BOOK', 0, '', '', '', 0);

select * from ext_mailbkg_mailbooking_dom emmd where emmd.bulk_customer_id ='1000002954'




		-----------
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
		te.event_type AS event_code,
		te.office_id AS event_office_facilty_id,
		te.office_name AS office_name,
		te.delivery_status AS event_description
	FROM mis_db.customer_xml_customer_mv AS cxcm
	INNER JOIN (
		SELECT t1.article_number, t1.event_date, t1.event_type, t1.office_id, t1.office_name, t1.delivery_status
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