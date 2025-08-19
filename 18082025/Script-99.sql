SELECT DISTINCT
    boc.article_number,
    boc.article_type,
    boc.art_status,
    COALESCE(
        CASE
            WHEN boc.article_type = 'BG' THEN bt.closed_from_office_name
            ELSE ais.booking_office_name
        END,
        ''
    ) AS booking_office_name,
    COALESCE(
        CASE
            WHEN boc.article_type = 'BG' THEN kom.pincode
            ELSE ais.destn_office_pin
        END,
        0
    ) AS destination_office_pin,
    boc.insured_flag,
    kar.is_recall,
    kar.is_return,
    boc.booking_reference_id,
    COALESCE(
        CASE
            WHEN boc.article_type = 'BG' THEN bt.bag_weight
            ELSE ais.article_weight
        END,
        0.0
    ) AS article_weight
FROM bagmgmt.bag_close_content AS boc
LEFT JOIN bagmgmt.kafka_article_recall_return AS kar
    ON boc.article_number = kar.article_number
LEFT JOIN LATERAL (
    SELECT
        ais1.booking_office_name,
        ais1.destn_office_pin,
        ais1.article_weight
    FROM bagmgmt.article_induction_stg AS ais1
    WHERE ais1.article_number = boc.article_number
      AND ais1.art_stg_date BETWEEN NOW() - INTERVAL '15 days' AND NOW()
    ORDER BY ais1.art_stg_date DESC
    LIMIT 1
) AS ais ON TRUE
LEFT JOIN bagmgmt.bag_transaction AS bt
    ON boc.article_number = bt.bag_number
LEFT JOIN bagmgmt.kafka_office_master AS kom
    ON bt.closed_to_office_id = kom.office_id
WHERE boc.bag_number = 'EBM1234567899'  -- Replace with actual bag number
  AND boc.set_date >= NOW() - INTERVAL '7 days';
