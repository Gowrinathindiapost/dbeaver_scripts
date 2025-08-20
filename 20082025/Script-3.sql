-- Queries generated from the Go code:

-- Insert into bagmgmt.bag_event (generated for each BagReceipt in BagReceipts)
INSERT INTO bagmgmt.bag_event (
    bag_number,
    transaction_date,
    bag_type,
    delivery_type,
    insured_flag,
    from_office_id,
    to_office_id,
    bag_weight,
    event_type,
    user_id,
    set_number,
    schedule_id,
    set_date
) VALUES (
    -- Values corresponding to BagReceipt fields will be inserted here.
    -- Example: 'BAG123', '2023-10-27 10:00:00', 'LB', 'Delivery', true, 101, 102, 5.0, 'RECEIVED', 1234, 'SET001', 'SCH001', '2023-10-27'
);

-- Insert into bagmgmt.insured_verification (conditional, based on InsuredFlag or BagType)
INSERT INTO bagmgmt.insured_verification (
    article_bag_number,
    bag_type,
    received_by,
    office,
    set_date,
    set_name,
    weight,
    event_type,
    art_type,
    approved_status,
    booking_office
) VALUES (
    -- Values corresponding to BagReceipt fields and computed values will be inserted here.
    -- Example: 'BAG123', 'LB', '1234', 102, '2023-10-27', 'SET001', 5.0, 'RECEIVED', 'BG', 'V', '101'
);

-- Select office_class from kafka_office_master (conditional, inside the loop)
SELECT coalesce(office_class, '') AS office_class
FROM kafka_office_master
WHERE office_id = -- BagReceipt.ToOfficeID value here;

-- Update latest events (performed by brr.bcnr.UpdateBagsLatestEvent, which is not shown in detail)
-- This would likely involve UPDATE statements on a table that stores the latest events.

-- Delete header and content (conditional, based on BagNumberUUID)
-- brr.bcstg.DeleteHeaderAndContentForReceive would perform DELETE statements.
-- The actual DELETE queries depend on the schema of the tables being accessed by brr.bcstg.DeleteHeaderAndContentForReceive.
-- Example (illustrative):
DELETE FROM some_header_table WHERE bag_uuid = 'BagNumberUUID';
DELETE FROM some_content_table WHERE bag_uuid = 'BagNumberUUID';



-----------------------------------
-- Insert into bagmgmt.bag_event

INSERT INTO bagmgmt.bag_event (
    bag_number,
    transaction_date,
    bag_type,
    delivery_type,
    insured_flag,
    from_office_id,
    to_office_id,
    bag_weight,
    event_type,
    user_id,
    set_number,
    schedule_id,
    set_date
) VALUES (
    'RBX0000000187',
    NOW(), -- Current timestamp for transaction_date
    'BO',
    'NTD',
    false,
    21280737,
    21282092,
    1000.0,
    'RO',
    50025669,
    'SET1',
    2608,
    '2025-03-06'
);

-- Select office_class from kafka_office_master

SELECT coalesce(office_class, '') AS office_class
FROM bagmgmt.kafka_office_master
WHERE office_id = 21860009 ; --21282092;
select * from bagmgmt.kafka_office_master

-- Conditional Insert into bagmgmt.insured_verification (based on insured_flag and bag_type)

-- The insured_flag is false, and the bag_type is not IB, so the insured_verification insert will not happen in this case.

-- Update Latest event.

-- This update is not possible to generate without knowing the schema and the exact logic of the brr.bcnr.UpdateBagsLatestEvent function, but it would be something like this:

-- UPDATE latest_event_table SET event_type = 'RO' WHERE bag_number = 'RBX0000000187';

-- Delete header and content.

-- Because bag_number_uuid is '00000000-0000-0000-0000-000000000001' the delete function will not be executed.

-- No delete operation is necessary.

------------
UPDATE bagmgmt.bag_transaction
SET
    last_event = temp_bag.eventtype,
    is_valid = temp_bag.isvalid
FROM (
    SELECT
        unnest(ARRAY['RBX0000000187']::text[]) AS bagnumber,
        unnest(ARRAY['RO']::text[]) AS eventtype,
        unnest(ARRAY[false]::bool[]) AS isvalid
) AS temp_bag
WHERE bag_number = temp_bag.bagnumber;

----
SELECT 
    di.from_office_id,
    di.from_office_name,
    di.arrival_date,
    di.departure_date,
    di.total_bags,
    di.total_weight,
    --MAX(ah.arrival_id) AS trip_id,
    ah.arrival_id AS trip_id,
    di.space_occupied,
    ah.total_bags_received,
    ah.total_weight AS arrival_weight
FROM bagmgmt.departure_info di
JOIN bagmgmt.arrival_header ah 
    ON di.sch_id = ah.schedule_id
-- AND di.from_office_id = ah.office_id  -- Uncomment if required
WHERE di.sch_id = '1854'
AND di.from_office_id = 21260551  -- Proper array format
AND di.trip_start_date >= '2024-06-26'  -- Correct date format
AND di.trip_start_date <= '2024-06-26'
GROUP BY 
    di.from_office_id, di.from_office_name, di.arrival_date, 
    di.departure_date, di.total_bags, di.total_weight, 
    di.space_occupied, ah.total_bags_received, ah.total_weight,ah.arrival_id;

SELECT 
    di.from_office_id,
    di.from_office_name,
    TO_CHAR(di.arrival_date, 'YYYY-MM-DD HH24:MI:SS') AS arrival_date,
    TO_CHAR(di.departure_date, 'YYYY-MM-DD HH24:MI:SS') AS departure_date,
    di.total_bags,
    di.total_weight,
    ah.arrival_id AS trip_id,
    di.space_occupied,
    ah.total_bags_received,
    ah.total_weight AS arrival_weight
FROM bagmgmt.departure_info di
JOIN bagmgmt.arrival_header ah 
    ON di.sch_id = ah.schedule_id
WHERE di.sch_id = '1854'
AND di.from_office_id = 21260551
AND di.trip_start_date::DATE = '2024-06-26';
