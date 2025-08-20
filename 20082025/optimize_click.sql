optimize TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R;
OPTIMIZE TABLE target_table FINAL;
OPTIMIZE TABLE mis_db.new_customer_tracking_event_mv FINAL;
OPTIMIZE TABLE mis_db.amazon_target_table FINAL;
OPTIMIZE TABLE mis_db.amazon_target_table_dt FINAL;
OPTIMIZE TABLE mis_db.amazon_target_table_dt_ib FINAL;
OPTIMIZE TABLE mis_db.amazon_target_table_ib FINAL;
OPTIMIZE TABLE mis_db.customer_log FINAL;
OPTIMIZE TABLE mis_db.customer_master FINAL;
OPTIMIZE TABLE mis_db.mv_booking_dom FINAL;
OPTIMIZE TABLE mis_db.mv_booking_intl FINAL;
OPTIMIZE TABLE mis_db.new_customer_tracking_event_mv FINAL;
OPTIMIZE TABLE mis_db.new_customer_tracking_event_new_mv FINAL;
OPTIMIZE TABLE mis_db.new_customer_xml_facility_customer_mv FINAL;
OPTIMIZE TABLE mis_db.new_customer_xml_facility_customer_new_mv FINAL;
OPTIMIZE TABLE mis_db.tracking_event_mv FINAL;



 
 
 
 
 


SELECT
    query_id,
    user,
    address,
    elapsed,
    memory_usage,
    query
FROM
    system.processes
ORDER BY
    elapsed DESC;


SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    is_done,
    latest_fail_reason
FROM
    system.mutations
WHERE
    database = 'mis_db' AND table = 'amazon_target_table_dt'
ORDER BY
    create_time DESC;