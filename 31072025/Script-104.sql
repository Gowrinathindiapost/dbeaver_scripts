SELECT DISTINCT ON (mncxfcmn.article_number) *
FROM mis_db.mv_new_customer_xml_facility_customer_mv_new AS mncxfcmn
WHERE booking_date > '2025-06-01 00:00:14.872'
ORDER BY mncxfcmn.article_number, mncxfcmn.booking_date; 

SELECT mncxfcmn.* FROM mis_db.mv_new_customer_xml_facility_customer_mv_new AS mncxfcmn
WHERE bulk_customer_id = 2000014074  AND (booking_date > '2025-06-01 00:00:14.872')
ORDER BY mncxfcmn.booking_date

SELECT DISTINCT ON (mncxfcmn.article_number) *
FROM mis_db.mv_new_customer_xml_facility_customer_mv_new AS mncxfcmn
WHERE bulk_customer_id = 2000014074  AND (booking_date > '2025-06-01 00:00:14.872')
ORDER BY mncxfcmn.article_number, mncxfcmn.booking_date; 