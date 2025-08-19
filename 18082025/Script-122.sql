-- Delete specific records
ALTER TABLE orders DELETE WHERE order_date < '2023-01-01';

-- Delete all data from table
ALTER TABLE amazon_target_table_dt DELETE WHERE 1=1;
optimize table 