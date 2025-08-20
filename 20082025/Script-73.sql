Select(
			"kmd.article_number AS ArticleNumber",
			"kmd.mail_type_code AS ArticleType",
			"to_char(kmd.md_created_date, 'DDMMYYYY') AS BookingDate",
			"to_char(kmd.md_created_date, 'HH24MISS') AS BookingTime",
			"kmd.md_office_id_bkg as BookingOfficeFacilityID",
			"kom.office_name as BookingOfficeName",
			"kmd.origin_pincode as BookingPIN",
			"kba1.city as SenderAddressCity",
			"kmd.destination_office_id as DestinationOfficeFacilityID",
			"kmd.destination_office_name as DestinationOfficeName",
			"kmd.destination_pincode as DestinationPIN",
			"kba2.city as DestinationCity",
			"'INDIA' as DestinationCountry",
			"kba2.addressee_name as ReceiverName",
			"''::text AS InvoiceNo", // 15
			"''::text AS LineItem",
			"kmd.charged_weight as WeightValue",
			"kcd.total_amount AS Tariff",
			"0.0 as CODAmount",
			"''::text AS BookingType",
			"''::text AS ContractNumber",
			"''::text AS Refrence",
		).
			From("trackandtrace.kafka_mailbooking_dom kmd").
			Join("trackandtrace.kafka_booking_address kba1 ON kmd.sender_address_reference = kba1.address_id").
			Join("trackandtrace.kafka_booking_address kba2 ON kmd.receiver_address_reference = kba2.address_id").
			Join("trackandtrace.kafka_charges_detail kcd ON kmd.charges_detail_id = kcd.charges_detail_id").
			Join("trackandtrace.kafka_office_master kom ON kmd.md_office_id_bkg = kom.office_id").
			LeftJoin("trackandtrace.kafka_article_event kat ON kmd.article_number = kat.article_number AND kat.event_code = 'ID'").
			Where(sq.Eq{"kmd.article_number": article_number}).
			Limit(1),

			querybag := psql.Select(
		"to_char(be.transaction_date, 'DDMMYYYY') AS EventDate",
		"to_char(be.transaction_date, 'HH24MISS') AS EventTime",
		"CASE WHEN be.event_type in ('CL','DI') THEN kom.office_name WHEN be.event_type in ('RO','OP','OR') THEN kom2.office_name ELSE NULL END AS EventOfficeName",
		"CASE WHEN be.event_type in ('CL','DI') THEN be.from_office_id WHEN be.event_type in ('RO','OP','OR') THEN be.to_office_id ELSE NULL END AS EventOfficeFaciltyID",
		"CASE WHEN be.event_type = 'CL' THEN 'BAG_CLOSE' WHEN be.event_type in('OP','OR') THEN 'BAG_OPEN' WHEN be.event_type = 'DI' THEN 'BAG_DISPATCH' WHEN be.event_type = 'RO' THEN 'TMO_RECEIVE' END AS EventCode",
		"CASE WHEN be.event_type = 'CL' THEN 'Bag Close' WHEN be.event_type in('OP','OR') THEN 'Bag Open' WHEN be.event_type = 'DI' THEN 'Bag Dispatch' WHEN be.event_type = 'RO' THEN 'Item Received' END AS EventDescription",
	).
		From("trackandtrace.kafka_bag_event be").
		Join("trackandtrace.kafka_office_master kom ON be.from_office_id = kom.office_id").
		Join("trackandtrace.kafka_office_master kom2 ON be.to_office_id = kom2.office_id").
		Join("(SELECT DISTINCT bag_number FROM trackandtrace.kafka_bag_close_content WHERE article_number = ? UNION SELECT DISTINCT bag_number FROM trackandtrace.kafka_bag_open_content WHERE article_number = ?) BagNumbers ON be.bag_number = BagNumbers.bag_number", article_number, article_number).
		OrderBy("be.transaction_date DESC").
		Limit(1)

	// 2. Try Article Event (if found, overrides Bag Event)
	queryArticleEvent := psql.Select(
		"to_char(kae.event_date, 'DDMMYYYY') AS EventDate",
		"to_char(kae.event_date, 'HH24MISS') AS EventTime",
		"kom.office_name AS EventOfficeName",
		"kae.current_office_id AS EventOfficeFaciltyID",
		"CASE WHEN kae.event_code = 'RC' THEN 'ITEM_RETURN'"+
			"WHEN kae.event_code = 'ID' THEN 'ITEM_DELIVERY'"+
			"WHEN kae.event_code = 'IN' THEN 'ITEM_BAGGING' "+
			"WHEN kae.event_code = 'RT' THEN 'ITEM_RETURN' "+
			"WHEN kae.event_code = 'DE' THEN 'ITEM_ONHOLD' "+
			"WHEN kae.event_code = 'RD' THEN 'ITEM_REDIRECT'"+
			"WHEN kae.event_code = 'IT' THEN 'ITEM_RETURN'			END AS EventCode",

		"CASE WHEN kae.event_code = 'RC' THEN 'Item Return'"+
			"WHEN kae.event_code = 'ID' THEN 'Item delivery'"+
			"WHEN kae.event_code = 'IN' THEN 'Item Bagging' "+
			"WHEN kae.event_code = 'RT' THEN 'Item Return' "+
			"WHEN kae.event_code = 'DE' THEN 'Item Onhold' "+
			"WHEN kae.event_code = 'RD' THEN 'Item redirect'"+
			"WHEN kae.event_code = 'IT' THEN 'Item Return'			END AS EventDescription",
	).
		From("trackandtrace.kafka_article_event kae").
		Join("trackandtrace.kafka_office_master kom ON kae.current_office_id = kom.office_id").
		Where(sq.Eq{"kae.article_number": article_number}).
		OrderBy("kae.event_date DESC").
		Limit(1)
	queryTransLEvent := psql.Select(
		"to_char(kat.created_date, 'DDMMYYYY') AS EventDate",
		"to_char(kat.created_date, 'HH24MISS') AS EventTime",
		"kat.source_office_name AS EventOfficeName",
		"kat.source_office_id AS EventOfficeFaciltyID",

		"CASE WHEN kat.action_code = 1 THEN 'ITEM_DELIVERY'"+
			"WHEN kat.action_code = 2 THEN 'ITEM_ONHOLD'"+
			"WHEN kat.action_code = 3 THEN 'ITEM_REDIRECT' "+
			"WHEN kat.action_code = 4 THEN 'ITEM_RETURN' "+
			"WHEN kat.action_code = 5 THEN 'ITEM_ONHOLD' "+
			"WHEN kat.action_code = 8 THEN 'ITEM_REDIRECT' "+
			"WHEN kat.action_code = 3 THEN 'ITEM_REDIRECT'"+
			"WHEN kat.action_code = 12 THEN 'ITEM_DELIVERY'"+
			"WHEN kat.action_code = 0 THEN 'ITEM_DELIVERY'"+
			"WHEN kat.action_code = 303 THEN 'ITEM_REDIRECT'			END AS EventCode",
		"CASE WHEN kat.action_code = 1 THEN 'Item delivery'"+
			"WHEN kat.action_code = 2 THEN 'Item Onhold'"+
			"WHEN kat.action_code = 3 THEN 'Item redirect'"+
			"WHEN kat.action_code = 4 THEN 'Item Return' "+
			"WHEN kat.action_code = 5 THEN 'Item Onhold' "+
			"WHEN kat.action_code = 8 THEN 'Item redirect' "+
			"WHEN kat.action_code = 12 THEN 'Item delivery'"+
			"WHEN kat.action_code = 0 THEN 'Item delivery'"+
			"WHEN kat.action_code = 303 THEN 'Item redirect' END AS EventDescription",
	).
		From("trackandtrace.kafka_article_transaction kat").
		Where(sq.Eq{"kat.article_number": article_number}).
		OrderBy("kat.created_date DESC").
		Limit(1)
		querydeliverystatus := psql.Select(
			"CASE WHEN action_code = 0 THEN 'delivered' WHEN action_code = 1 THEN 'delivered' WHEN action_code = 2 THEN 'Item on hold' WHEN action_code = 4 THEN 'Item returned' WHEN action_code = 5 THEN 'Item on hold' WHEN action_code = 6 THEN 'Item on hold' WHEN action_code = 3 THEN 'Item redirected' WHEN action_code = 8 THEN 'Item redirected' WHEN action_code = 303 THEN 'Item redirected' ELSE 'not delivered' END AS DelStat",
		).
			From("trackandtrace.kafka_article_transaction kat").
			Where(sq.Eq{"kat.article_number": article_number})
		
}


SELECT count(*) FROM mis_db.tracking_event_mv tem WHERE event_date LIKE '1970%'
SELECT count(*)
FROM mis_db.tracking_event_mv AS tem
WHERE toYear(event_date) = 1970

SELECT count(*)
FROM mis_db.tracking_event_mv AS tem
WHERE toString(event_date) LIKE '1970%'
select * from mis_db.tracking_event_mv where 


ALTER TABLE mis_db.tracking_event_mv
DELETE WHERE toString(event_date) LIKE '1970%'

select * from mis_db.tracking_event_mv tem where tem.article_number ='EK386633306IN'
select * from mis_db.ext_bagmgmt_bag_close_content where article_number ='EK386633306IN'
select * from mis_db.ext_bagmgmt_bag_close_content where bag_number='CBK3018507773'
select * from mis_db.ext_bagmgmt_bag_close_content where date(_peerdb_synced_at)='2025-06-24'
SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    is_done,
    latest_fail_reason,
    parts_to_do_unfetched,
    parts_to_do_fetched,
    parts_to_do_processed
FROM system.mutations
WHERE database = 'mis_db' AND table = 'tracking_event_mv'
ORDER BY create_time DESC
LIMIT 10;

select * from mis_db.tracking_event_mv where article_number='EZ771735211IN'
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
WHERE cxcm.bulk_customer_id = '1000002954'
  AND parseDateTimeBestEffort(cxcm.booking_date) < now()
  AND (el.generation_time IS NULL 
       OR toDateTime(te.event_date) > toDateTime(el.generation_time))
       
       select * from mis_db.tracking_event_mv where article_number IN 
       (select article_number from customer_xml_customer_mv cxcm where bulk_customer_id ='1000002954')
       
       
    SELECT
    *
FROM mis_db.tracking_event_mv
WHERE article_number IN (
    SELECT article_number
    FROM mis_db.customer_xml_customer_mv cxcm
    WHERE bulk_customer_id = '1000002954'
)
