OPTIMIZE TABLE mis_db.tracking_event_mv ON CLUSTER cluster_1S_2R FINAL;
OPTIMIZE TABLE mis_db.tracking_event_mv  FINAL;

clickhouse-client --query "SELECT * FROM system.zookeeper WHERE path = '/'"


SELECT hostName(), name 
FROM clusterAllReplicas('cluster_1S_2R', system.tables)
WHERE database = 'mis_db' 
  AND name = 'tracking_event_mv';


OPTIMIZE TABLE mis_db.new_customer_tracking_event_new_mv ON CLUSTER cluster_1S_2R FINAL;
OPTIMIZE TABLE mis_db.new_customer_tracking_event_new_mv  FINAL;
OPTIMIZE TABLE mis_db.new_customer_tracking_event_mv ON CLUSTER cluster_1S_2R FINAL;
OPTIMIZE TABLE mis_db.new_customer_tracking_event_mv FINAL;--
OPTIMIZE TABLE mis_db.new_customer_xml_facility_customer_mv FINAL;
OPTIMIZE TABLE mis_db.new_customer_xml_facility_customer_new_mv FINAL;


