explain PIPELINE 
SELECT distinct
    be.article_number AS article_number,
    be.event_date AS event_date,

    multiIf(
        be.status = 'AC', 'Item Address Change',
        be.status = 'AP', 'Article Priority Set',
        be.status = 'BC', 'Item Barcode Change',
        be.status = 'BD', 'Beat Dispatch',
        be.status = 'CA', 'Close Verification Appro',
        be.status = 'CH', 'Close Verification On Ho',
        be.status = 'D',  'Item Delivered',
        be.status = 'DL', 'Item Delay',
        be.status = 'DM', 'Damaged',
        be.status = 'F',  'Item Delivered',
        be.status = 'FO', 'Found',
        be.status = 'IB', 'Item Booked',
        be.status = 'ID', 'Item Dispatched',
        be.status = 'IR', 'Item Received',
        be.status = 'LO', 'Lost',
        be.status = 'MI', 'Not Received',
        be.status = 'MS', 'Miss Sorted',
        be.status = 'O',  'Item OnHold',
        be.status = 'OH', 'Item OnHold',
        be.status = 'RA', 'Receive Verification App',
        be.status = 'RH', 'Receive Verification On',
        be.status = 'RM', 'Item Remove',
        be.status = 'RN', 'Item Return',
        be.status = 'RT', 'Item Redirect',
        be.status = 'MR', 'Item Misroute',
        be.status = 'CN', 'Item Cancel',
        be.status = 'N',  'Item Not Delivered',
        be.status = 'G',  'Item Not Delivered',
        be.status = 'MT', 'Item Missent Receipt',
        be.status = 'CS', 'Custom Receive',
        be.status = 'CO', 'Custom Hold',
        be.status = 'CR', 'Custom Return',
        be.status = 'X',  'Item Print',
        be.status = 'IG', 'Item Bagging',
        be.status = 'UP', 'Under Paid',
        be.status = 'OB', 'ONDC Booked',
        ''
    ) AS event_type,

    be.event_ofc_facility_id AS office_id,
    'csi_bag_header' AS source_table,

    CASE
        WHEN be.status IN ('D', 'F', 'DE', 'ID', 'IR') THEN 'Delivered'
        WHEN be.reason_code = '01' THEN 'Door Locked'
        WHEN be.reason_code = '02' THEN 'Addressee moved'
        WHEN be.reason_code = '03' THEN 'On Addressee Instructions'
        WHEN be.reason_code = '04' THEN 'Poste Restante'
        WHEN be.reason_code = '05' THEN 'Local Holiday'
        WHEN be.reason_code = '06' THEN 'Insufficient Address'
        WHEN be.reason_code = '07' THEN 'Addressee cannot be located'
        WHEN be.reason_code = '08' THEN 'Addressee Absent'
        WHEN be.reason_code = '09' THEN 'Refused'
        WHEN be.reason_code = '10' THEN 'Held by Customs'
        WHEN be.reason_code = '11' THEN 'Addressee Left without instructions'
        WHEN be.reason_code = '12' THEN 'Missed Delivery'
        WHEN be.reason_code = '13' THEN 'Damaged'
        WHEN be.reason_code = '14' THEN 'No such person in the address'
        WHEN be.reason_code = '15' THEN 'Office closed'
        WHEN be.reason_code = '16' THEN 'Divert to BO'
        WHEN be.reason_code = '17' THEN 'Unclaimed'
        WHEN be.reason_code = '18' THEN 'Deceased'
        WHEN be.reason_code = '19' THEN 'Beat Change'
        WHEN be.reason_code = '20' THEN 'Divert to Bulk Delivery'
        WHEN be.reason_code = '21' THEN 'Addressee has P.O.Box'
        WHEN be.reason_code = '22' THEN 'Addressee request own pick-up'
        WHEN be.reason_code = '23' THEN 'Prohibited articles'
        WHEN be.reason_code = '24' THEN 'Prohibited Articles or Leaky Contents'
        WHEN be.reason_code = '25' THEN 'Others'
        WHEN be.reason_code = '26' THEN 'Force majeure - item not delivered'
        WHEN be.reason_code = '27' THEN 'Validity Period Exceeded'
        WHEN be.reason_code = '28' THEN 'Insufficient information to complete tran'
        WHEN be.reason_code = '29' THEN 'Wrong or missing address zip code'
        WHEN be.reason_code = '30' THEN 'Missent'
        WHEN be.reason_code = '31' THEN 'Returns not available now'
        WHEN be.reason_code = '32' THEN 'Revert from Bulk Addressee'
        WHEN be.reason_code = '33' THEN 'Divert to Bulk Addressee'
        WHEN be.reason_code = '34' THEN 'Unclaimed By Remitter'
        WHEN be.reason_code = '35' THEN 'Intimation Delivered'
        WHEN be.reason_code = '36' THEN 'Payment of charges'
        WHEN be.reason_code = '37' THEN 'Addressee has P.O.Box'
        WHEN be.reason_code = '38' THEN 'No Service Provided'
        WHEN be.reason_code = '39' THEN 'Divert to Beat'
        WHEN be.reason_code = '40' THEN 'Recalled'
        WHEN be.reason_code = '41' THEN 'Based on customer request'
        ELSE ''
    END AS delivery_status,

    10 AS sort_order

FROM mis_db.csi_article_event AS be
LEFT JOIN mis_db.csi_article_item  cai ON (cai.bag_item = be.article_number);
--EXPLAIN PLAN — shows the logical execution plan
--EXPLAIN PIPELINE — shows the physical execution pipeline, operators, and threads
--EXPLAIN AST — shows the parsed abstract syntax tree
--EXPLAIN SYNTAX — shows the final SQL as ClickHouse will execute it
