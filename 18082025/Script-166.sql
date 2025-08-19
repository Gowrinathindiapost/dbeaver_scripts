SELECT
    CASE
        WHEN action_code = 1 THEN 'delivered'
        ELSE 'not delivered'
    END AS DelStat
FROM
    trackandtrace.kafka_article_transaction
WHERE
    article_number = 'RT738876445IN';


SELECT
    CASE
        WHEN EXISTS (
            -- Subquery to check for any delivered status (action_code = 1)
            -- for the specified article number.
            SELECT 1
            FROM trackandtrace.kafka_article_transaction
            WHERE article_number = 'RT738876445IN' AND action_code = 1
        ) THEN 'delivered'
        ELSE 'not delivered'
    END AS DelStat;
