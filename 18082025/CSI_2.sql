CREATE TABLE mis_db.status_event_type
(
    status_code String,
    event_type String
)
ENGINE = TinyLog; -- For small tables, or use MergeTree if you prefer


INSERT INTO mis_db.status_event_type VALUES
('AC', 'Item Address Change'),
('AP', 'Article Priority Set'),
('BC', 'Item Barcode Change'),
('BD', 'Beat Dispatch'),
('CA', 'Close Verification Appro'),
('CH', 'Close Verification On Ho'),
('D',  'Item Delivered'),
('DL', 'Item Delay'),
('DM', 'Damaged'),
('F',  'Item Delivered'),
('FO', 'Found'),
('IB', 'Item Booked'),
('ID', 'Item Dispatched'),
('IR', 'Item Received'),
('LO', 'Lost'),
('MI', 'Not Received'),
('MS', 'Miss Sorted'),
('O',  'Item OnHold'),
('OH', 'Item OnHold'),
('RA', 'Receive Verification App'),
('RH', 'Receive Verification On'),
('RM', 'Item Remove'),
('RN', 'Item Return'),
('RT', 'Item Redirect'),
('MR', 'Item Misroute'),
('CN', 'Item Cancel'),
('N',  'Item Not Delivered'),
('G',  'Item Not Delivered'),
('MT', 'Item Missent Receipt'),
('CS', 'Custom Receive'),
('CO', 'Custom Hold'),
('CR', 'Custom Return'),
('X',  'Item Print'),
('IG', 'Item Bagging'),
('UP', 'Under Paid'),
('OB', 'ONDC Booked');


CREATE TABLE mis_db.reason_delivery_status
(
    reason_code String,
    delivery_status String
)
ENGINE = TinyLog;


INSERT INTO mis_db.reason_delivery_status VALUES
('01', 'Door Locked'),
('02', 'Addressee moved'),
('03', 'On Addressee Instructions'),
('04', 'Poste Restante'),
('05', 'Local Holiday'),
('06', 'Insufficient Address'),
('07', 'Addressee cannot be located'),
('08', 'Addressee Absent'),
('09', 'Refused'),
('10', 'Held by Customs'),
('11', 'Addressee Left without instructions'),
('12', 'Missed Delivery'),
('13', 'Damaged'),
('14', 'No such person in the address'),
('15', 'Office closed'),
('16', 'Divert to BO'),
('17', 'Unclaimed'),
('18', 'Deceased'),
('19', 'Beat Change'),
('20', 'Divert to Bulk Delivery'),
('21', 'Addressee has P.O.Box'),
('22', 'Addressee request own pick-up'),
('23', 'Prohibited articles'),
('24', 'Prohibited Articles or Leaky Contents'),
('25', 'Others'),
('26', 'Force majeure - item not delivered'),
('27', 'Validity Period Exceeded'),
('28', 'Insufficient information to complete tran'),
('29', 'Wrong or missing address zip code'),
('30', 'Missent'),
('31', 'Returns not available now'),
('32', 'Revert from Bulk Addressee'),
('33', 'Divert to Bulk Addressee'),
('34', 'Unclaimed By Remitter'),
('35', 'Intimation Delivered'),
('36', 'Payment of charges'),
('37', 'Addressee has P.O.Box'),
('38', 'No Service Provided'),
('39', 'Divert to Beat'),
('40', 'Recalled'),
('41', 'Based on customer request');



SELECT DISTINCT
    be.article_number,
    be.event_date,
    st.event_type,
    be.event_ofc_facility_id AS office_id,
    'csi_bag_header' AS source_table,
    -- Use COALESCE to handle special cases (status-delivered logic), and join with reason code mapping
    COALESCE(
        CASE WHEN be.status IN ('D', 'F', 'DE', 'ID', 'IR')
            THEN 'Delivered'
        ELSE rd.delivery_status END,
        ''
    ) AS delivery_status,
    10 AS sort_order
FROM mis_db.csi_article_event be
LEFT JOIN mis_db.csi_article_item cai
    ON cai.bag_item = be.article_number
LEFT JOIN mis_db.status_event_type st
    ON be.status = st.status_code
LEFT JOIN mis_db.reason_delivery_status rd
    ON be.reason_code = rd.reason_code
