WITH schedule_data AS (
            SELECT schedule_start_time::text as schedule_start_time,
                   source_facility_id,
                   destination_facility_id
            FROM carriermgmt.schedule
            WHERE schedule_id = $1
        ),
        sequence_data AS (
            SELECT COALESCE(MAX(sequence_number), 0) AS max_seq,
                   MAX(departure_time) AS departure_time,
                   MAX(arrival_time) AS arrival_time,
                   MAX(stop_office_id) AS stop_office_id,
                   MAX(next_stop_office_id) AS next_stop_office_id
            FROM carriermgmt.schedule_stop_sequence_stg
            WHERE schedule_id = $1
            AND sequence_number = (
                SELECT COALESCE(MAX(sequence_number), 0)
                FROM carriermgmt.schedule_stop_sequence_stg
                WHERE schedule_id = $1
            )
        )
        SELECT
            s.schedule_start_time,
            s.source_facility_id,
            s.destination_facility_id,
            COALESCE(sq.max_seq, 0),
            sq.departure_time,
            sq.arrival_time,
            sq.stop_office_id,
            sq.next_stop_office_id
        FROM schedule_data s
        LEFT JOIN sequence_data sq ON true