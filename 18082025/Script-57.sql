SELECT *
FROM bagmgmt.kafka_article_transaction
WHERE current_office_id = '21070745'
  AND (is_invoiced = TRUE);