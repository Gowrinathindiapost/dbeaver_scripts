SELECT 
    XMLELEMENT(
        NAME "Schedules",
        XMLAGG(
            XMLELEMENT(
                NAME "Schedule",
                XMLELEMENT(NAME "schedule_id", s.schedule_id),
                XMLELEMENT(NAME "schedule_name", s.schedule_name),
                XMLELEMENT(NAME "schedule_type", s.schedule_type),
                XMLELEMENT(NAME "bag_type", 
                    (SELECT XMLAGG(XMLELEMENT(NAME "item", item))
                     FROM unnest(s.bag_type) AS item)
                ),
                XMLELEMENT(NAME "source_facility_id", s.source_facility_id),
                XMLELEMENT(NAME "source_facility_name", s.source_facility_name),
                XMLELEMENT(NAME "destination_facility_id", s.destination_facility_id),
                XMLELEMENT(NAME "destination_facility_name", s.destination_facility_name),
                XMLELEMENT(NAME "schedule_start_time", s.schedule_start_time),
                XMLELEMENT(NAME "schedule_create_office_id", s.schedule_create_office_id),
                XMLELEMENT(NAME "schedule_valid_from", s.schedule_valid_from),
                XMLELEMENT(NAME "schedule_valid_to", s.schedule_valid_to),
                XMLELEMENT(NAME "transport_mode", s.transport_mode),
                XMLELEMENT(NAME "schedule_running_days", 
                    (SELECT XMLAGG(XMLELEMENT(NAME "day", day))
                     FROM unnest(s.schedule_running_days) AS day)
                ),
                XMLELEMENT(NAME "schedule_status", s.schedule_status),
                XMLELEMENT(NAME "created_by", s.created_by),
                XMLELEMENT(NAME "created_date", s.created_date),
                XMLELEMENT(NAME "updated_by", COALESCE(s.updated_by, '')),
                XMLELEMENT(NAME "updated_date", COALESCE(s.updated_date::text, '')),
                XMLELEMENT(NAME "deleted_by", COALESCE(s.deleted_by, '')),
                XMLELEMENT(NAME "deleted_date", COALESCE(s.deleted_date::text, ''))
            )
        )
    ) AS xml_output
FROM carriermgmt.schedule s;