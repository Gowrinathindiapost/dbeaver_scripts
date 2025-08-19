SELECT
    query_id,
    user,
    elapsed,
    read_rows,
    read_bytes,
    memory_usage,
    query
FROM system.processes
ORDER BY elapsed DESC
LIMIT 10;






    INSERT INTO mis_db.raw_mailbooking
    
    SELECT
        ebmd.mailbooking_dom_id AS mailbooking_id,
        physical_weight,
        dimension_length,
        dimension_breadth,
        dimension_height,
        volumetric_weight,
        charged_weight,
        CAST(greatCircleDistance(dmo2.latitude, dmo2.longitude, dmo.latitude, dmo.longitude) / 1000 AS Int32) AS distance,
        bulk_customer_id,
        customer_name,
        contract_id,
        priority_flag,
        instructions_delivery,
        instructions_rts,
        sender_pincode,
        receiver_pincode,
        pickup_flag,
        pickup_addr_ref_sender,
        alt_addr_flag,
        alt_addr_ref_sender,
        article_number,
        bkg_ref_id,
        destination_office_id,
        destination_office_name,
        dmo.office_type_id AS destination_office_type_id,
        dmo.office_type_code AS destination_office_type_code,
        destination_pincode,
        md_office_id_bkg,
        origin_office_name,
        dmo2.office_type_id AS origin_office_type_id,
        dmo.office_type_code AS origin_office_type_code,
        origin_pincode,
        pickup_office_pincode,
        pickup_office_id,
        pickup_office_name,
        pickup_schedule_slot,
        pickup_schedule_date,
        ebcd.charges_detail_id AS charges_detail_id,
        pg_tran_ref,
        kafka_updated_date,
        bulk_ref,
        mail_shape_code,
        dbbm.master_description AS shape_description,
        ebmd.mail_type_code AS mail_type_code,
        product_id,
        product_name,
        ebmd.booking_type_code,
        dbbm1.master_description AS booking_type_description,
        mail_form_code,
        delivery_slot,
        ebmd.payment_mode_code AS payment_mode_code,
        dbbm2.master_description AS payment_mode_description,
        ebmd.status_code AS status_code,
        dbbm3.master_description AS status_description,
        ebmd.md_counter_no AS md_counter_no,
        ebmd.md_shift_no AS md_shift_no,
        ebmd.md_created_date AS md_created_date,
        ebmd.md_created_date AS trans_time,
        DATE(md_created_date) AS trans_date,
        ebmd.md_created_by AS md_created_by,
        md_ip_address_bkg,
        md_updated_date,
        md_updated_by,
        md_updated_office_id,
        md_updated_ip_address,
        md_user_type_code,
        user_description,
        md_channel_type_code,
        channel_description,
        sender_address_reference,
        receiver_address_reference,
        auth_status,
        md_auth_by,
        md_auth_date,
        md_auth_office_id,
        md_auth_ip_address,
        cover_type,
        ins_article_weight,
        false AS return_to_customer,
        '' AS destination_ccode,
        '' AS destination_cname,
        0 AS declared_value,
        '' AS sender_country_name,
        '' AS sender_country_code,
        '' AS receiver_country_code,
        '' AS receiver_country_name,
        '' AS pbe_bank_ref,
        0 AS pbe_no,
        '' AS pbe_type_code,
        0 AS upload_doc_inv_count,
        0 AS upload_doc_cert_count,
        0 AS upload_doc_lic_count,
        false AS declaration1,
        false AS declaration2,
        false AS declaration4,
        false AS declaration3,
        '' AS iec_code,
        false AS self_filing_cus_broker,
        '' AS cus_broker_lic_no,
        '' AS cus_broker_name,
        '' AS cus_broker_address,
        '' AS origin_country_code,
        '' AS origin_country_name,
        '' AS mail_class_code,
        '' AS class_description,
        '' AS mail_nature_type_code,
        '' AS nature_type_description,
        '' AS mail_transport_type_code,
        '' AS transport_type_description,
        '' AS non_dely_instns_code,
        '' AS non_dely_instns_description,
        0 AS subpiece_count,
        false AS is_drawback_opted,
        tax_amount,
        vp_cod_charge,
        insurance_type,
        value_insurance,
        insurance_charge,
        pod_ack_charge,
        door_delivery_charge,
        pickup_charge,
        premailing_charge,
        parcel_packing_charge_with_box,
        parcel_packing_charge_without_box,
        parcel_packing_bubble_wrap_charge,
        parcel_packing_service_charge,
        cgst,
        sgst,
        igst,
        base_amount,
        total_amount,
        ebcd.prepayment_type_code,
        dbbm4.master_description AS prepayment_type_description,
        prepayment_value,
        ebcd.vp_cod_type_code,
        dbbm5.master_description AS vp_cod_type_description,
        vp_cod_value,
        md_cd_created_date,
        md_cd_created_by,
        md_cd_updated_date,
        utgst,
        bkg_office_gst_no,
        sender_gst_no,
        receiver_gst_no,
        ams_charge,
        1 AS book_table,
        now64() AS load_date
    FROM (
        SELECT *,
            row_number() OVER (PARTITION BY mailbooking_dom_id ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.ext_mailbkg_mailbooking_dom
    ) AS ebmd
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY charges_detail_id ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.ext_mailbkg_charges_detail
    ) AS ebcd ON ebmd.charges_detail_id = ebcd.charges_detail_id
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY customer_id ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.dim_customermgmt_customer
    ) AS dcc ON ebmd.bulk_customer_id = dcc.customer_id
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY office_id ORDER BY master_updated_date DESC) AS rn
        FROM mis_db.dim_mdm_office
    ) AS dmo ON dmo.office_id = ebmd.destination_office_id
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY office_id ORDER BY master_updated_date DESC) AS rn
        FROM mis_db.dim_mdm_office
    ) AS dmo2 ON dmo2.office_id = ebmd.md_office_id_bkg
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY product_id ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.dim_mdm_product_master
    ) AS dmpm ON dmpm.product_code = ebmd.mail_type_code
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY user_code ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.dim_mailbkg_user_type
    ) AS dbut ON dbut.user_code = ebmd.md_user_type_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm ON dbbm.master_type_reference = 'mail_shape_cd' AND dbbm.bs_master_status = 'A' AND dbbm.master_code = ebmd.mail_shape_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm1 ON dbbm1.master_type_reference = 'booking_type_cd' AND dbbm1.bs_master_status = 'A' AND dbbm1.master_code = ebmd.booking_type_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm2 ON dbbm2.master_type_reference = 'payment_type_cd' AND dbbm2.bs_master_status = 'A' AND dbbm2.master_code = ebmd.payment_mode_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm3 ON dbbm3.master_type_reference = 'status_cd' AND dbbm3.bs_master_status = 'A' AND dbbm3.master_code = ebmd.status_code
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY channel_code ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.dim_mailbkg_channel_type
    ) AS dbct ON dbct.channel_code = ebmd.md_channel_type_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm4 ON dbbm4.master_type_reference = 'prepayment_type_cd' AND dbbm4.bs_master_status = 'A' AND dbbm4.master_code = ebcd.prepayment_type_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm5 ON dbbm5.master_type_reference = 'vp_cod_type_cd' AND dbbm5.bs_master_status = 'A' AND dbbm5.master_code = ebcd.vp_cod_type_code
    WHERE ebmd.status_code = 'PC'
    
    
    
    INSERT INTO mis_db.fct_crm_raw_bulk_last_event
    (customer_id,  
    customer_name,  
    customer_type_id,  
    customer_type,  
    date_key,  
    full_dt,  
    book_channel_id,  
    book_channel_name,  
    book_type_id,  
    book_type_name,  
    user_id,  
    user_name,  
    article_number,  
    article_type,  
    booking_date_time,  
    booking_office_pin,  
    booking_office_name,  
    destination_pin,  
    destination_office_name,  
    art_event_date_time,  
    art_event_code,  
    event_description,  
    event_office_name,  
    non_delivery_reason_description  )  
    
    WITH latestevent AS 
    (SELECT 
        innerQ.art_number as article_number,        
        innerQ.art_event_date_time AS art_event_date_time,        
        innerQ.art_event_code AS art_event_code,        
        innerQ.art_office_id AS art_office_id,        
        innerQ.art_office_name AS art_office_name,        
        innerQ.art_remarks AS art_remarks,        
        innerQ.event_description AS event_description,        
        ROW_NUMBER() OVER (PARTITION BY innerQ.art_number 
            ORDER BY innerQ.art_event_date_time DESC) AS rnn      
    FROM (SELECT        
        article_number as art_number,        
        event_date AS art_event_date_time,        
        event_code AS art_event_code,        
        current_office_id AS art_office_id,        
        current_office_name AS art_office_name,        
        remarks AS art_remarks,        
        dpec.event_description as event_description          
        FROM mis_db.ext_pdmanagement_article_event epae final        
        LEFT JOIN mis_db.dim_pdmanagement_event_code dpec final           
        ON dpec.event_code = epae.event_code       
    WHERE event_code <> 'RC'            
        union all        
    SELECT           
        ebboc.article_number AS art_number,          
        ebbe.transaction_date AS art_event_date_time,          
        ebbe.event_type AS art_event_code,          
        multiIf(ebbe.event_type IN ('OP','OR','RO','RF'),dmot.office_id, dmof.office_id) AS art_office_id,    
        multiIf(ebbe.event_type IN ('OP','OR','RO','RF'),dmot.office_name,dmof.office_name) AS art_office_name,          
        '' AS art_remarks,          
        dbem.event_description AS event_description        
    FROM mis_db.ext_bagmgmt_bag_event ebbe final         
    LEFT JOIN mis_db.dim_bagmgmt_event_master dbem 
    final ON dbem.event_code = ebbe.event_type        
    LEFT JOIN mis_db.ext_bagmgmt_bag_open_content ebboc 
    final ON ebboc.bag_number = ebbe.bag_number        
    LEFT JOIN mis_db.ext_bagmgmt_bag_close_content ebbcc 
    final ON ebbcc.bag_number = ebbe.bag_number 
    AND ebboc.article_number = ebbcc.article_number        
    LEFT JOIN mis_db.dim_mdm_office dmot 
    final ON dmot.office_id=ebbe.to_office_id        
    LEFT JOIN mis_db.dim_mdm_office dmof 
    final ON dmof.office_id=ebbe.from_office_id) AS innerQ)  
    SELECT distinct  
        custdet.customer_id AS customer_id,    
        custdet.customer_name AS customer_name,    
        custdet.customer_type_ids AS customer_type_id,    
        custdet.customer_type_names AS customer_type,    
        dt.date_key AS date_key,    
        dt.full_dt AS full_dt,    
        dbc.channel_code AS book_channel_id,    
        dbc.channel_description AS book_channel_name,    
        dbt.master_code AS book_type_id,    
        dbt.master_description AS book_type_name,    
        du.employee_id AS user_id,    
        du.employee_first_name || ' ' || du.employee_middle_name || ' ' || du.employee_last_name AS user_name,    
        bkng.article_number AS article_number,    
        pdct.product_code AS article_type,    
        toString(formatDateTime(bkng.md_created_date, '%Y/%m/%dT%H:%i:%S')) AS booking_date_time,    
        bkng.origin_pincode AS booking_office_pin,    
        bkng.origin_office_name AS booking_office_name,    
        bkng.destination_pincode AS destination_pin,    
        bkng.destination_office_name AS destination_office_name,
        (CASE 
            WHEN DATE(lte.art_event_date_time)='1970-01-01' THEN bkng.md_created_date    
            ELSE lte.art_event_date_time END)  AS art_event_date_time,    
        (CASE 
            WHEN lte.art_event_code ='' THEN 'BOK'    
            ELSE lte.art_event_code END) AS art_event_code,    
        (CASE 
            WHEN lte.event_description ='' THEN 'Item Booked'    
            ELSE lte.event_description END) AS event_description,    
        (CASE 
            WHEN lte.art_office_name IS NULL THEN bkng.origin_office_name    
            ELSE lte.art_office_name END) AS event_office_name,    
        (CASE 
            WHEN dprm.remarks='' THEN lte.art_remarks      
            ELSE dprm.remarks END) AS non_delivery_reason_description  
        FROM mis_db.raw_mailbooking AS bkng  JOIN (SELECT 
            cust.customer_id,           
            cust.customer_name,           
            cust.customer_type_ids,           
            arrayStringConcat(groupArray(dct.customer_type_name), ',') AS customer_type_names    
            FROM (SELECT 
                customer_id,             
                customer_name,             
                customer_type_ids,             
                JSONExtractArrayRaw(customer_type_ids) AS arr      
                FROM mis_db.dim_customermgmt_customer    ) AS cust    
            ARRAY JOIN arr AS type_id    LEFT JOIN mis_db.dim_customermgmt_allcustomer_type AS dct      
            ON JSONExtractString(type_id) = dct.customer_type_id    
            GROUP BY 
                cust.customer_id, 
                cust.customer_name, 
                cust.customer_type_ids  ) AS custdet    
            ON custdet.customer_id = bkng.bulk_customer_id  LEFT JOIN latestevent AS lte    
            ON bkng.article_number = lte.article_number    AND lte.rnn = 1  
            LEFT JOIN mis_db.dim_mailbkg_channel_type AS dbc    
            ON dbc.channel_code = bkng.md_channel_type_code  
            LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbt    
            ON dbt.master_code = bkng.booking_type_code       
            AND dbt.master_type_reference = 'booking_type_cd'  
            LEFT JOIN mis_db.dim_pis_employee_master AS du    
            ON CAST(du.employee_id AS String) = bkng.md_created_by  
            LEFT JOIN mis_db.dim_mdm_product_master AS pdct    
            ON bkng.product_id = pdct.product_id  
            LEFT JOIN (SELECT *, ROW_NUMBER() OVER 
            (PARTITION BY article_number ORDER BY created_date DESC) AS rn      
        FROM mis_db.ext_pdmanagement_article_transaction FINAL) AS expat    
        ON lte.article_number = expat.article_number and expat.rn=1  
        LEFT JOIN mis_db.dim_time AS dt ON DATE(bkng.md_cd_created_date) = dt.full_dt  
        LEFT JOIN mis_db.dim_pdmanagement_remarks_master AS dprm    
        ON expat.remarks_id = dprm.remarks_id  
        WHERE    bkng.status_code = 'PC'
    
    SELECT count(*) FROM (
    WITH latestevent AS 
    (SELECT 
        innerQ.art_number as article_number,        
        innerQ.art_event_date_time AS art_event_date_time,        
        innerQ.art_event_code AS art_event_code,        
        innerQ.art_office_id AS art_office_id,        
        innerQ.art_office_name AS art_office_name,        
        innerQ.art_remarks AS art_remarks,        
        innerQ.event_description AS event_description,        
        ROW_NUMBER() OVER (PARTITION BY innerQ.art_number 
            ORDER BY innerQ.art_event_date_time DESC) AS rnn      
    FROM (SELECT        
        article_number as art_number,        
        event_date AS art_event_date_time,        
        event_code AS art_event_code,        
        current_office_id AS art_office_id,        
        current_office_name AS art_office_name,        
        remarks AS art_remarks,        
        dpec.event_description as event_description          
        FROM mis_db.ext_pdmanagement_article_event epae final        
        LEFT JOIN mis_db.dim_pdmanagement_event_code dpec final           
        ON dpec.event_code = epae.event_code       
    WHERE event_code <> 'RC'            
        union all        
    SELECT           
        ebboc.article_number AS art_number,          
        ebbe.transaction_date AS art_event_date_time,          
        ebbe.event_type AS art_event_code,          
        multiIf(ebbe.event_type IN ('OP','OR','RO','RF'),dmot.office_id, dmof.office_id) AS art_office_id,    
        multiIf(ebbe.event_type IN ('OP','OR','RO','RF'),dmot.office_name,dmof.office_name) AS art_office_name,          
        '' AS art_remarks,          
        dbem.event_description AS event_description        
    FROM mis_db.ext_bagmgmt_bag_event ebbe final         
    LEFT JOIN mis_db.dim_bagmgmt_event_master dbem 
    final ON dbem.event_code = ebbe.event_type        
    LEFT JOIN mis_db.ext_bagmgmt_bag_open_content ebboc 
    final ON ebboc.bag_number = ebbe.bag_number        
    LEFT JOIN mis_db.ext_bagmgmt_bag_close_content ebbcc 
    final ON ebbcc.bag_number = ebbe.bag_number 
    AND ebboc.article_number = ebbcc.article_number        
    LEFT JOIN mis_db.dim_mdm_office dmot 
    final ON dmot.office_id=ebbe.to_office_id        
    LEFT JOIN mis_db.dim_mdm_office dmof 
    final ON dmof.office_id=ebbe.from_office_id) AS innerQ)  
    SELECT distinct  
        custdet.customer_id AS customer_id,    
        custdet.customer_name AS customer_name,    
        custdet.customer_type_ids AS customer_type_id,    
        custdet.customer_type_names AS customer_type,    
        dt.date_key AS date_key,    
        dt.full_dt AS full_dt,    
        dbc.channel_code AS book_channel_id,    
        dbc.channel_description AS book_channel_name,    
        dbt.master_code AS book_type_id,    
        dbt.master_description AS book_type_name,    
        du.employee_id AS user_id,    
        du.employee_first_name || ' ' || du.employee_middle_name || ' ' || du.employee_last_name AS user_name,    
        bkng.article_number AS article_number,    
        pdct.product_code AS article_type,    
        toString(formatDateTime(bkng.md_created_date, '%Y/%m/%dT%H:%i:%S')) AS booking_date_time,    
        bkng.origin_pincode AS booking_office_pin,    
        bkng.origin_office_name AS booking_office_name,    
        bkng.destination_pincode AS destination_pin,    
        bkng.destination_office_name AS destination_office_name,
        (CASE 
            WHEN DATE(lte.art_event_date_time)='1970-01-01' THEN bkng.md_created_date    
            ELSE lte.art_event_date_time END)  AS art_event_date_time,    
        (CASE 
            WHEN lte.art_event_code ='' THEN 'BOK'    
            ELSE lte.art_event_code END) AS art_event_code,    
        (CASE 
            WHEN lte.event_description ='' THEN 'Item Booked'    
            ELSE lte.event_description END) AS event_description,    
        (CASE 
            WHEN lte.art_office_name IS NULL THEN bkng.origin_office_name    
            ELSE lte.art_office_name END) AS event_office_name,    
        (CASE 
            WHEN dprm.remarks='' THEN lte.art_remarks      
            ELSE dprm.remarks END) AS non_delivery_reason_description  
        FROM mis_db.raw_mailbooking AS bkng  JOIN (SELECT 
            cust.customer_id,           
            cust.customer_name,           
            cust.customer_type_ids,           
            arrayStringConcat(groupArray(dct.customer_type_name), ',') AS customer_type_names    
            FROM (SELECT 
                customer_id,             
                customer_name,             
                customer_type_ids,             
                JSONExtractArrayRaw(customer_type_ids) AS arr      
                FROM mis_db.dim_customermgmt_customer    ) AS cust    
            ARRAY JOIN arr AS type_id    LEFT JOIN mis_db.dim_customermgmt_allcustomer_type AS dct      
            ON JSONExtractString(type_id) = dct.customer_type_id    
            GROUP BY 
                cust.customer_id, 
                cust.customer_name, 
                cust.customer_type_ids  ) AS custdet    
            ON custdet.customer_id = bkng.bulk_customer_id  LEFT JOIN latestevent AS lte    
            ON bkng.article_number = lte.article_number    AND lte.rnn = 1  
            LEFT JOIN mis_db.dim_mailbkg_channel_type AS dbc    
            ON dbc.channel_code = bkng.md_channel_type_code  
            LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbt    
            ON dbt.master_code = bkng.booking_type_code       
            AND dbt.master_type_reference = 'booking_type_cd'  
            LEFT JOIN mis_db.dim_pis_employee_master AS du    
            ON CAST(du.employee_id AS String) = bkng.md_created_by  
            LEFT JOIN mis_db.dim_mdm_product_master AS pdct    
            ON bkng.product_id = pdct.product_id  
            LEFT JOIN (SELECT *, ROW_NUMBER() OVER 
            (PARTITION BY article_number ORDER BY created_date DESC) AS rn      
        FROM mis_db.ext_pdmanagement_article_transaction FINAL) AS expat    
        ON lte.article_number = expat.article_number and expat.rn=1  
        LEFT JOIN mis_db.dim_time AS dt ON DATE(bkng.md_cd_created_date) = dt.full_dt  
        LEFT JOIN mis_db.dim_pdmanagement_remarks_master AS dprm    
        ON expat.remarks_id = dprm.remarks_id  
        WHERE    bkng.status_code = 'PC'
    )
    
    
    
    INSERT INTO mis_db.raw_mailbooking
    
    SELECT
        ebmd.mailbooking_dom_id AS mailbooking_id,
        physical_weight,
        dimension_length,
        dimension_breadth,
        dimension_height,
        volumetric_weight,
        charged_weight,
        CAST(greatCircleDistance(dmo2.latitude, dmo2.longitude, dmo.latitude, dmo.longitude) / 1000 AS Int32) AS distance,
        bulk_customer_id,
        customer_name,
        contract_id,
        priority_flag,
        instructions_delivery,
        instructions_rts,
        sender_pincode,
        receiver_pincode,
        pickup_flag,
        pickup_addr_ref_sender,
        alt_addr_flag,
        alt_addr_ref_sender,
        article_number,
        bkg_ref_id,
        destination_office_id,
        destination_office_name,
        dmo.office_type_id AS destination_office_type_id,
        dmo.office_type_code AS destination_office_type_code,
        destination_pincode,
        md_office_id_bkg,
        origin_office_name,
        dmo2.office_type_id AS origin_office_type_id,
        dmo.office_type_code AS origin_office_type_code,
        origin_pincode,
        pickup_office_pincode,
        pickup_office_id,
        pickup_office_name,
        pickup_schedule_slot,
        pickup_schedule_date,
        ebcd.charges_detail_id AS charges_detail_id,
        pg_tran_ref,
        kafka_updated_date,
        bulk_ref,
        mail_shape_code,
        dbbm.master_description AS shape_description,
        ebmd.mail_type_code AS mail_type_code,
        product_id,
        product_name,
        ebmd.booking_type_code,
        dbbm1.master_description AS booking_type_description,
        mail_form_code,
        delivery_slot,
        ebmd.payment_mode_code AS payment_mode_code,
        dbbm2.master_description AS payment_mode_description,
        ebmd.status_code AS status_code,
        dbbm3.master_description AS status_description,
        ebmd.md_counter_no AS md_counter_no,
        ebmd.md_shift_no AS md_shift_no,
        ebmd.md_created_date AS md_created_date,
        ebmd.md_created_date AS trans_time,
        DATE(md_created_date) AS trans_date,
        ebmd.md_created_by AS md_created_by,
        md_ip_address_bkg,
        md_updated_date,
        md_updated_by,
        md_updated_office_id,
        md_updated_ip_address,
        md_user_type_code,
        user_description,
        md_channel_type_code,
        channel_description,
        sender_address_reference,
        receiver_address_reference,
        auth_status,
        md_auth_by,
        md_auth_date,
        md_auth_office_id,
        md_auth_ip_address,
        cover_type,
        ins_article_weight,
        false AS return_to_customer,
        '' AS destination_ccode,
        '' AS destination_cname,
        0 AS declared_value,
        '' AS sender_country_name,
        '' AS sender_country_code,
        '' AS receiver_country_code,
        '' AS receiver_country_name,
        '' AS pbe_bank_ref,
        0 AS pbe_no,
        '' AS pbe_type_code,
        0 AS upload_doc_inv_count,
        0 AS upload_doc_cert_count,
        0 AS upload_doc_lic_count,
        false AS declaration1,
        false AS declaration2,
        false AS declaration4,
        false AS declaration3,
        '' AS iec_code,
        false AS self_filing_cus_broker,
        '' AS cus_broker_lic_no,
        '' AS cus_broker_name,
        '' AS cus_broker_address,
        '' AS origin_country_code,
        '' AS origin_country_name,
        '' AS mail_class_code,
        '' AS class_description,
        '' AS mail_nature_type_code,
        '' AS nature_type_description,
        '' AS mail_transport_type_code,
        '' AS transport_type_description,
        '' AS non_dely_instns_code,
        '' AS non_dely_instns_description,
        0 AS subpiece_count,
        false AS is_drawback_opted,
        tax_amount,
        vp_cod_charge,
        insurance_type,
        value_insurance,
        insurance_charge,
        pod_ack_charge,
        door_delivery_charge,
        pickup_charge,
        premailing_charge,
        parcel_packing_charge_with_box,
        parcel_packing_charge_without_box,
        parcel_packing_bubble_wrap_charge,
        parcel_packing_service_charge,
        cgst,
        sgst,
        igst,
        base_amount,
        total_amount,
        ebcd.prepayment_type_code,
        dbbm4.master_description AS prepayment_type_description,
        prepayment_value,
        ebcd.vp_cod_type_code,
        dbbm5.master_description AS vp_cod_type_description,
        vp_cod_value,
        md_cd_created_date,
        md_cd_created_by,
        md_cd_updated_date,
        utgst,
        bkg_office_gst_no,
        sender_gst_no,
        receiver_gst_no,
        ams_charge,
        1 AS book_table,
        now64() AS load_date
    FROM (
        SELECT *,
            row_number() OVER (PARTITION BY mailbooking_dom_id ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.ext_mailbkg_mailbooking_dom
    ) AS ebmd
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY charges_detail_id ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.ext_mailbkg_charges_detail
    ) AS ebcd ON ebmd.charges_detail_id = ebcd.charges_detail_id
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY customer_id ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.dim_customermgmt_customer
    ) AS dcc ON ebmd.bulk_customer_id = dcc.customer_id
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY office_id ORDER BY master_updated_date DESC) AS rn
        FROM mis_db.dim_mdm_office
    ) AS dmo ON dmo.office_id = ebmd.destination_office_id
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY office_id ORDER BY master_updated_date DESC) AS rn
        FROM mis_db.dim_mdm_office
    ) AS dmo2 ON dmo2.office_id = ebmd.md_office_id_bkg
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY product_id ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.dim_mdm_product_master
    ) AS dmpm ON dmpm.product_code = ebmd.mail_type_code
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY user_code ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.dim_mailbkg_user_type
    ) AS dbut ON dbut.user_code = ebmd.md_user_type_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm ON dbbm.master_type_reference = 'mail_shape_cd' AND dbbm.bs_master_status = 'A' AND dbbm.master_code = ebmd.mail_shape_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm1 ON dbbm1.master_type_reference = 'booking_type_cd' AND dbbm1.bs_master_status = 'A' AND dbbm1.master_code = ebmd.booking_type_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm2 ON dbbm2.master_type_reference = 'payment_type_cd' AND dbbm2.bs_master_status = 'A' AND dbbm2.master_code = ebmd.payment_mode_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm3 ON dbbm3.master_type_reference = 'status_cd' AND dbbm3.bs_master_status = 'A' AND dbbm3.master_code = ebmd.status_code
    LEFT JOIN (
        SELECT *,
            row_number() OVER (PARTITION BY channel_code ORDER BY _peerdb_synced_at DESC) AS rn
        FROM mis_db.dim_mailbkg_channel_type
    ) AS dbct ON dbct.channel_code = ebmd.md_channel_type_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm4 ON dbbm4.master_type_reference = 'prepayment_type_cd' AND dbbm4.bs_master_status = 'A' AND dbbm4.master_code = ebcd.prepayment_type_code
    LEFT JOIN mis_db.dim_mailbkg_bs_master AS dbbm5 ON dbbm5.master_type_reference = 'vp_cod_type_cd' AND dbbm5.bs_master_status = 'A' AND dbbm5.master_code = ebcd.vp_cod_type_code
    WHERE ebmd.status_code = 'PC'
    
    
    INSERT INTO `ext_pdmanagement_article_event` (`article_event_id`,`article_number`,`event_code`,`event_date`,`remarks`,`created_by`,`current_office_pincode`,`current_office_id`,`current_office_name`,`redirected_office_pincode`,`redirected_office_id`,`redirected_office_name`,`beat_name`,`postman_name`,`batch_name`,`data_source`,`_peerdb_is_deleted`,`_peerdb_version`)  SELECT JSONExtract(_peerdb_data, 'article_event_id', 'Int64') AS `article_event_id`,JSONExtract(_peerdb_data, 'article_number', 'String') AS `article_number`,JSONExtract(_peerdb_data, 'event_code', 'String') AS `event_code`,parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, 'event_date'),6) AS `event_date`,JSONExtract(_peerdb_data, 'remarks', 'String') AS `remarks`,JSONExtract(_peerdb_data, 'created_by', 'String') AS `created_by`,JSONExtract(_peerdb_data, 'current_office_pincode', 'Int32') AS `current_office_pincode`,JSONExtract(_peerdb_data, 'current_office_id', 'Int32') AS `current_office_id`,JSONExtract(_peerdb_data, 'current_office_name', 'String') AS `current_office_name`,JSONExtract(_peerdb_data, 'redirected_office_pincode', 'Int32') AS `redirected_office_pincode`,JSONExtract(_peerdb_data, 'redirected_office_id', 'Int32') AS `redirected_office_id`,JSONExtract(_peerdb_data, 'redirected_office_name', 'String') AS `redirected_office_name`,JSONExtract(_peerdb_data, 'beat_name', 'String') AS `beat_name`,JSONExtract(_peerdb_data, 'postman_name', 'String') AS `postman_name`,JSONExtract(_peerdb_data, 'batch_name', 'String') AS `batch_name`,JSONExtract(_peerdb_data, 'data_source', 'String') AS `data_source`,intDiv(_peerdb_record_type, 2) AS `_peerdb_is_deleted`,_peerdb_timestamp AS `_peerdb_version` FROM `_peerdb_raw_mirrorpdmgmtv2` WHERE _peerdb_batch_id > 10402 AND _peerdb_batch_id <= 10403 AND  _peerdb_destination_table_name = 'ext_pdmanagement_article_event'
    
    
    SELECT
    query_id,
    user,
    elapsed,
    read_rows,
    read_bytes,
    memory_usage,
    query
FROM system.processes --ere query_id='3c1c8f20-3624-4e42-bf99-8e40dc594d12'
ORDER BY elapsed DESC
LIMIT 10

