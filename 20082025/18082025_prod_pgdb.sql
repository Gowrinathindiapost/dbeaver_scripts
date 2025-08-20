select * from trackandtrace.kafka_article_transaction 
select article_number from trackandtrace.kafka_mailbooking_dom where bulk_customer_id='2000014074'


-- List all article_numbers
SELECT article_number
FROM trackandtrace.kafka_mailbooking_dom
WHERE bulk_customer_id = '2000014074';

-- Count of article_numbers
SELECT count(*) AS total
FROM trackandtrace.kafka_mailbooking_dom
WHERE bulk_customer_id = '2000014074';--total articles booked by PMV : 298587


SELECT
    groupArray(article_number) AS article_numbers,  -- collects into an array
    count() AS total
FROM trackandtrace.kafka_mailbooking_dom
WHERE bulk_customer_id = '2000014074';

select * from trackandtrace.kafka_article_transaction  where action_code =1 and article_number in (
SELECT article_number
FROM trackandtrace.kafka_mailbooking_dom
WHERE bulk_customer_id = '2000014074'
)

select article_number from trackandtrace.kafka_article_transaction where action_code=1


SELECT *
FROM trackandtrace.kafka_article_transaction at
WHERE at.action_code = 1
  AND at.article_number IN (
      SELECT article_number
      FROM trackandtrace.kafka_mailbooking_dom
      WHERE bulk_customer_id = '2000014074'
  );


SELECT count(*)
FROM trackandtrace.kafka_article_transaction at
WHERE at.action_code = 1
  AND at.article_number IN (
      SELECT article_number
      FROM trackandtrace.kafka_mailbooking_dom
      WHERE bulk_customer_id = '2000014074'
  );---62913 delivered
