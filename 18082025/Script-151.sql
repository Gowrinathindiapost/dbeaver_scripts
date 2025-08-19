SELECT 
			article_number,
			 event_date,
			 office_id,
			office_name AS event_office,
			event_type
		FROM mis_db.tracking_event_mv
		WHERE article_number = 'EU688599795IN'
		ORDER BY event_date ASC
		
		
		SELECT 
			article_number,
			bookedat,
			bookedon,
			toString(destination_pincode),
			toFloat64(tariff),
			article_type,
			delivery_location,
			'' as source_country,
            '' as destination_country
		FROM mis_db.mv_booking_dom
		WHERE article_number = 'EU688599795IN'

		UNION ALL

		SELECT 
			article_number,
			bookedat,
			bookedon,
			'999999' AS destination_pincode,
			toFloat64(tariff),
			article_type,
			delivery_location,
			ifNull(source_country, '') AS source_country,
            ifNull(destination_country, '') AS destination_country
		FROM mis_db.mv_booking_intl
		WHERE article_number = 'EU688599795IN'