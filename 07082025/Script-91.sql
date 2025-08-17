SELECT 
			kmd.article_number AS ArticleNumber,
			kmd.origin_office_name AS Bookedat,
			kmd.md_created_date AS Bookedon,
			kmd.destination_pincode AS DestinationPincode,
			kcd.total_amount AS Tariff,
			kmd.mail_type_code AS ArticleType,
			kmd.destination_office_name AS DeliveryLocation,
			kat.event_date AS DeliveryConfirmedOn
		FROM trackandtrace.kafka_mailbooking_dom kmd
		JOIN trackandtrace.kafka_charges_detail kcd ON kmd.charges_detail_id = kcd.charges_detail_id
		LEFT JOIN trackandtrace.kafka_article_event kat ON kmd.article_number = kat.article_number AND kat.event_code = 'ID'
		WHERE kmd.article_number = 