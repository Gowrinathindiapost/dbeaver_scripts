SELECT
			DATE(t1.event_time) AS Date,
			TO_CHAR(t1.event_time, 'HH24:MI:SS') AS Time,
			t3.ooe_name AS Office,
			'99999999' AS OfficeID,
			t2.event_name AS Event
		FROM trackandtrace.kafka_ips_event t1
		JOIN trackandtrace.ips_event_master  t2 ON t1.event_cd = t2.event_cd
		JOIN trackandtrace.ooe_master AS t3 ON t1.office_cd = t3.ooe_fcd
		WHERE t1.article_number = 'RR561826399IN'
		ORDER BY Date, Time;

select * from trackandtrace.kafka_ips_event t1 WHERE t1.article_number = 'RR561826399IN'  
3468799
3468799
3468799
3468799