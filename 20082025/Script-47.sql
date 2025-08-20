select * from sortmgmt.employee_scheduling es where role_group_allocated='Supervisor' in ('Supervisor') ilike '%uper%'
and duty_list_id(select dulty_list_id from sortmgmt.employee_scheduling_header where )

SELECT 
    
    es.employee_id,
    es.employee_designation,
        es.employee_name
    
FROM 
    sortmgmt.employee_scheduling es
WHERE 
(    'Supervisor','SUPER') = ANY (es.role_group_allocated);

select * 
    --es.employee_id,
    --es.employee_designation,
    --es.employee_name
FROM 
    sortmgmt.employee_scheduling es
WHERE 
    es.role_group_allocated && ARRAY['Supervisor', 'SUPER']::text[];
