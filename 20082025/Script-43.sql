select * from bagmgmt.bag_event where bag_number ='LBK6016945529'
SELECT
    be.bag_number AS BagNumber,
    be.set_date AS SetDate,
    be.transaction_date AS TransactionDate,
    be.event_type AS EventType,
    be.set_number AS SetNumber,
    be.bag_type AS BagType,
    om.office_name AS OfficeName,
    be.from_office_id AS FromOfficeID,
    be.bag_weight AS BagWeight
FROM
    bagmgmt.bag_event be
JOIN
    bagmgmt.kafka_office_master om ON be.from_office_id = om.office_id
WHERE
    be.bag_number = 'LBK6016945529'
    AND be.to_office_id = '21661278'
    AND be.event_type IN ('RO', 'RF', 'TO', 'TF', 'DP');
select * from kafka_office_master kom  where office_id='21661278'