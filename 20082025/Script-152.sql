SELECT article_number
FROM mis_db.csi_article_event cae
WHERE article_number IN (
    SELECT article_number
    FROM mis_db.csi_article_item cai
    WHERE bag_id IN (
        SELECT bag_id
        FROM mis_db.csi_bag_header
    )
);