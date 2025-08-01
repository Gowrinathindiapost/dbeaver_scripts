SELECT 
    vo.maintenance_interval_km, 
    vo.vehicle_reg_number, 
    v.trip_end_reading, 
    v.trip_end_date, 
    vm.last_maintenance_reading
FROM 
    carriermgmt.vehicle vo
JOIN 
    carriermgmt.vehicle_trip v ON vo.vehicle_reg_number = v.vehicle_reg_number
LEFT JOIN 
    carriermgmt.vehicle_maintenance vm ON vo.vehicle_reg_number = vm.vehicle_reg_number
    AND vm.vehicle_maintenance_id = (
        SELECT MAX(vm2.vehicle_maintenance_id) 
        FROM carriermgmt.vehicle_maintenance vm2 
        WHERE vm2.vehicle_reg_number = vo.vehicle_reg_number
    )
WHERE 
    (v.trip_end_reading - COALESCE(vm.last_maintenance_reading, 0)) >= vo.maintenance_interval_km
    AND vo.vehicle_status <> 'MAINTENANCE'
    AND vo.maintenance_mms IN ($1)
ORDER BY 
    vo.vehicle_reg_number DESC
OFFSET 
    $2
LIMIT 
    $3
    -----------------------
    SELECT 
    vo.maintenance_interval_km, 
    vo.vehicle_reg_number, 
    v.trip_end_reading, 
    v.trip_end_date, 
    vm.last_maintenance_reading
FROM 
    carriermgmt.vehicle vo
JOIN 
    carriermgmt.schedule_vehicle_driver_mapping v ON vo.vehicle_reg_number = v.vehicle_reg_number
LEFT JOIN 
    carriermgmt.vehicle_maintenance vm ON vo.vehicle_reg_number = vm.vehicle_reg_number
    AND vm.vehicle_maintenance_id = (
        SELECT MAX(vm2.vehicle_maintenance_id) 
        FROM carriermgmt.vehicle_maintenance vm2 
        WHERE vm2.vehicle_reg_number = vo.vehicle_reg_number
    )
WHERE 
    (v.trip_end_reading - COALESCE(vm.last_maintenance_reading, 0)) >= vo.maintenance_interval_km
    AND vo.vehicle_status <> 'MAINTENANCE'
    AND vo.maintenance_mms IN ($1)
ORDER BY 
    vo.vehicle_reg_number DESC
OFFSET 
    $2
LIMIT 
    $3