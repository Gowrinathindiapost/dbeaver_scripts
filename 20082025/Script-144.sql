 SELECT * FROM (
                        SELECT kmd.article_number AS "ArticleNumber", kmd.origin_office_name AS "Bookedat", kmd.md_created_date AS "Bookedon",
                               kmd.destination_pincode AS "DestinationPincode", kcd.total_amount AS "Tariff",
                               kmd.mail_type_code AS "ArticleType", kmd.destination_office_name AS "DeliveryLocation",    
                               kat.event_date AS "DeliveryConfirmedOn", 1 AS priority
                        FROM trackandtrace.kafka_mailbooking_dom kmd
                        LEFT JOIN trackandtrace.kafka_charges_detail kcd ON kmd.charges_detail_id = kcd.charges_detail_id 
                        LEFT JOIN trackandtrace.kafka_article_event kat ON kmd.article_number = kat.article_number AND kat.event_code = 'ID'
                        WHERE kmd.article_number = $1

                        UNION ALL

                        SELECT kmd.article_number, kom.office_name, kmd.induction_pos_date, kmd.destination_pin, kmd.total_tariff,
                               kmd.mail_service_type, '' AS DeliveryLocation, kat.event_date, 2
                        FROM trackandtrace.kafka_induction_domestic kmd
                        LEFT JOIN trackandtrace.kafka_article_event kat ON kmd.article_number = kat.article_number AND kat.event_code = 'ID'
                        JOIN trackandtrace.kafka_office_master kom ON kmd.office_id_induction = kom.office_id
                        WHERE kmd.article_number = $1

                        UNION ALL

                        SELECT kmd.article_number, kom.office_name, kmd.induction_pos_date, '000000', kmd.total_tariff,   
                               kmd.mail_service_type, '' AS DeliveryLocation, kat.event_date, 3
                        FROM trackandtrace.kafka_induction_international kmd
                        LEFT JOIN trackandtrace.kafka_article_event kat ON kmd.article_number = kat.article_number AND kat.event_code = 'ID'
                        JOIN trackandtrace.kafka_office_master kom ON kmd.office_id_induction = kom.office_id
                        WHERE kmd.article_number = $1
                ) AS bookings
                ORDER BY priority
                LIMIT 1 | Args: [RK123456781IN]
Executing SQL: {
WITH bagged AS (
        SELECT DISTINCT bag_number
        FROM trackandtrace.kafka_bag_close_content
        WHERE article_number = $1
),
received AS (
        SELECT DISTINCT bag_number
        FROM trackandtrace.kafka_bag_open_content
        WHERE article_number = $2
)
SELECT
        DATE(be.transaction_date) AS Date,
        to_char(be.transaction_date, 'HH24:MI:SS') AS Time,
        CASE
                WHEN be.event_type IN ('CL', 'DI') THEN kom.office_name
                WHEN be.event_type IN ('OP', 'OR') THEN kom2.office_name
                ELSE NULL
        END AS Office,
        CASE
                WHEN be.event_type = 'CL' THEN 'Item Bagged'
                WHEN be.event_type IN ('OP', 'OR') THEN 'Item Received'
                WHEN be.event_type = 'DI' THEN 'Item Dispatched'
        END AS Event
FROM trackandtrace.kafka_bag_event be
LEFT JOIN trackandtrace.kafka_office_master kom ON be.from_office_id = kom.office_id
LEFT JOIN trackandtrace.kafka_office_master kom2 ON be.to_office_id = kom2.office_id
LEFT JOIN bagged bcl ON be.bag_number = bcl.bag_number AND be.event_type IN ('CL', 'DI')
LEFT JOIN received bo ON be.bag_number = bo.bag_number AND be.event_type IN ('OP', 'OR')
WHERE
        (be.event_type IN ('CL', 'DI') AND bcl.bag_number IS NOT NULL)
        OR
        (be.event_type IN ('OP', 'OR') AND bo.bag_number IS NOT NULL)
ORDER BY Date, Time
 --[RK123456781IN RK123456781IN]} | Args: [RK123456781IN]
--Executing SQL: {
WITH bagged AS (
        SELECT DISTINCT bag_number
        FROM trackandtrace.kafka_bag_close_content
        WHERE article_number = $1
),
received AS (
        SELECT DISTINCT bag_number
        FROM trackandtrace.kafka_bag_open_content
        WHERE article_number = $1
)
SELECT
        DATE(be.transaction_date) AS Date,
        to_char(be.transaction_date, 'HH24:MI:SS') AS Time,
        CASE
                WHEN be.event_type IN ('CL', 'DI') THEN kom.office_name
                WHEN be.event_type IN ('OP', 'OR') THEN kom2.office_name
                ELSE NULL
        END AS Office,
        CASE
                WHEN be.event_type = 'CL' THEN 'Item Bagged'
                WHEN be.event_type IN ('OP', 'OR') THEN 'Item Received'
                WHEN be.event_type = 'DI' THEN 'Item Dispatched'
        END AS Event
FROM trackandtrace.kafka_bag_event be
LEFT JOIN trackandtrace.kafka_office_master kom ON be.from_office_id = kom.office_id
LEFT JOIN trackandtrace.kafka_office_master kom2 ON be.to_office_id = kom2.office_id
LEFT JOIN bagged bcl ON be.bag_number = bcl.bag_number AND be.event_type IN ('CL', 'DI')
LEFT JOIN received bo ON be.bag_number = bo.bag_number AND be.event_type IN ('OP', 'OR')
WHERE
        (be.event_type IN ('CL', 'DI') AND bcl.bag_number IS NOT NULL)
        OR
        (be.event_type IN ('OP', 'OR') AND bo.bag_number IS NOT NULL)
ORDER BY Date, Time