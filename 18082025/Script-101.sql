WITH latest_event AS (
	SELECT DISTINCT ON (bag_number)
		bag_number,
		ml_id,
		schedule_id,
		from_office_id,
		to_office_id,
		bag_weight,
		bag_type,
		insured_flag,
		delivery_type,
		event_type,
		transaction_date,
		set_date
	FROM bagmgmt.bag_event
	WHERE set_date >= DATE '2025-07-20'
	  AND event_type = 'DI'
	  AND to_office_id = '21630039'
	ORDER BY bag_number, transaction_date DESC
)
SELECT 
	ld.ml_id AS MailID,
	ml.schedule_name AS ScheduleName,
	ld.schedule_id AS ScheduleID,
	ld.bag_number AS BagNumber,
	ld.from_office_id AS FromOfficeID,
	ld.to_office_id AS ToOfficeID,
	omf.office_name AS FromOfficeName,
	subquery_cl.closefromofficeid AS ClosedFromOfficeID,
	close_from_office.office_name AS ClosedFromOfficeName,
	subquery_cl.close_to_office_id AS ClosedToOfficeID,
	close_to_office.office_name AS ClosedToOfficeName,
	ld.bag_weight AS BagWeight,
	ld.bag_type AS BagType,
	ld.insured_flag AS InsuredFlag,
	ld.delivery_type AS DeliveryType,
	ld.event_type AS EventType,
	ld.transaction_date AS TransactionDate
FROM latest_event ld
LEFT JOIN (
	SELECT 
		b1.bag_number, 
		b1.from_office_id AS close_from_office_id, 
		b1.to_office_id AS close_to_office_id, 
		b1.from_office_id AS closefromofficeid
	FROM bagmgmt.bag_event b1
	WHERE b1.event_type = 'CL'
	  AND b1.set_date >= DATE '2025-07-20'
) AS subquery_cl ON ld.bag_number = subquery_cl.bag_number
JOIN bagmgmt.kafka_office_master omf ON ld.from_office_id = omf.office_id
JOIN bagmgmt.kafka_office_master omt ON ld.to_office_id = omt.office_id
JOIN bagmgmt.mail_list ml ON ld.ml_id = ml.mail_id AND ld.schedule_id = ml.schedule_id
LEFT JOIN bagmgmt.kafka_office_master close_from_office ON subquery_cl.close_from_office_id = close_from_office.office_id
LEFT JOIN bagmgmt.kafka_office_master close_to_office ON subquery_cl.close_to_office_id = close_to_office.office_id
WHERE ld.set_date >= omt.rollout_date
ORDER BY ld.ml_id;


--21460006
--21630039

WITH latest_event AS (
	SELECT DISTINCT ON (bag_number)
		bag_number,
		ml_id,
		schedule_id,
		from_office_id,
		to_office_id,
		bag_weight,
		bag_type,
		insured_flag,
		delivery_type,
		event_type,
		transaction_date,
		set_date
	FROM bagmgmt.bag_event
	WHERE set_date >= DATE '2025-07-20'  -- Replace with actual setDate
	ORDER BY bag_number, transaction_date DESC
),
latest_di AS (
	SELECT *
	FROM latest_event
	WHERE event_type = 'DI'
	  AND to_office_id = '21630039'  -- Replace with actual toOfficeID
)
SELECT 
	ld.ml_id AS MailID,
	ml.schedule_name AS ScheduleName,
	ld.schedule_id AS ScheduleID,
	ld.bag_number AS BagNumber,
	ld.from_office_id AS FromOfficeID,
	ld.to_office_id AS ToOfficeID,
	omf.office_name AS FromOfficeName,
	subquery_cl.closefromofficeid AS ClosedFromOfficeID,
	close_from_office.office_name AS ClosedFromOfficeName,
	subquery_cl.close_to_office_id AS ClosedToOfficeID,
	close_to_office.office_name AS ClosedToOfficeName,
	ld.bag_weight AS BagWeight,
	ld.bag_type AS BagType,
	ld.insured_flag AS InsuredFlag,
	ld.delivery_type AS DeliveryType,
	ld.event_type AS EventType,
	ld.transaction_date AS TransactionDate
FROM latest_di ld
LEFT JOIN (
	SELECT 
		b1.bag_number, 
		b1.from_office_id AS close_from_office_id, 
		b1.to_office_id AS close_to_office_id, 
		b1.from_office_id AS closefromofficeid
	FROM bagmgmt.bag_event b1
	WHERE b1.event_type = 'CL'
	  AND b1.set_date >= DATE '2025-07-20'  -- Same interval for pruning
) AS subquery_cl ON ld.bag_number = subquery_cl.bag_number
JOIN bagmgmt.kafka_office_master omf ON ld.from_office_id = omf.office_id
JOIN bagmgmt.kafka_office_master omt ON ld.to_office_id = omt.office_id
JOIN bagmgmt.mail_list ml ON ld.ml_id = ml.mail_id AND ld.schedule_id = ml.schedule_id
LEFT JOIN bagmgmt.kafka_office_master close_from_office ON subquery_cl.close_from_office_id = close_from_office.office_id
LEFT JOIN bagmgmt.kafka_office_master close_to_office ON subquery_cl.close_to_office_id = close_to_office.office_id
WHERE ld.set_date >= omt.rollout_date
ORDER BY ld.ml_id;
