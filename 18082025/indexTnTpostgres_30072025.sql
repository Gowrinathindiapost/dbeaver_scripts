----index to be created on trackandtracedb prod postgres 

-- Main event table
CREATE INDEX idx_bag_event_lookup
ON trackandtrace.kafka_bag_event (bag_number, transaction_date, event_type, from_office_id, to_office_id);

-- Bag content tables
CREATE INDEX idx_bag_close_content_article_bag
ON trackandtrace.kafka_bag_close_content (article_number, bag_number);

CREATE INDEX idx_bag_open_content_article_bag
ON trackandtrace.kafka_bag_open_content (article_number, bag_number);

-- Office master
CREATE INDEX idx_office_master_office_id
ON trackandtrace.kafka_office_master (office_id);--available



--optin2 chatgpt
CREATE INDEX idx_kbcc_article_number ON trackandtrace.kafka_bag_close_content(article_number);
CREATE INDEX idx_kboc_article_number ON trackandtrace.kafka_bag_open_content(article_number);
CREATE INDEX idx_kbe_bag_number ON trackandtrace.kafka_bag_event(bag_number);
CREATE INDEX idx_kom_office_id ON trackandtrace.kafka_office_master(office_id);
CREATE INDEX idx_kbe_transaction_date_expr ON trackandtrace.kafka_bag_event (DATE(transaction_date));
CREATE INDEX idx_kbe_transaction_date ON trackandtrace.kafka_bag_event (transaction_date);



---perplex

-- Bag close/open content for fast article lookups:
CREATE INDEX idx_bag_close_content_article_number ON trackandtrace.kafka_bag_close_content(article_number);--already available kafka_bag_close_content_article_number_idx
CREATE INDEX idx_bag_open_content_article_number  ON trackandtrace.kafka_bag_open_content(article_number);--already available kafka_bag_open_content_article_number_idx

-- Bag events for join and order:
CREATE INDEX idx_bag_event_bag_number ON trackandtrace.kafka_bag_event(bag_number);--available kafka_bag_event_bag_number_idx
-- For combining filter+sort (if high volume):
CREATE INDEX idx_bag_event_bag_number_transaction_date ON trackandtrace.kafka_bag_event(bag_number, transaction_date);--available idx_bag_event_bag_event_date

-- Event to office joins (optional, usually these are not a performance bottleneck):
CREATE INDEX idx_bag_event_from_office_id ON trackandtrace.kafka_bag_event(from_office_id);--available idx_bag_event_from_office
CREATE INDEX idx_bag_event_to_office_id   ON trackandtrace.kafka_bag_event(to_office_id);--available idx_bag_event_to_office

-- Office master (PK):
CREATE UNIQUE INDEX idx_office_master_office_id ON trackandtrace.kafka_office_master(office_id);--available idx_office_master_id
