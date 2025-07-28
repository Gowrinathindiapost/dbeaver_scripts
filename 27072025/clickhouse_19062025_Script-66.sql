-- mis_db.ext_moneyremit_mobooking_ap definition
--mirrormoneyremittrack
--mirrormailbkgtrack

drop TABLE mis_db.ext_moneyremit_mobooking_ap;
drop TABLE mis_db.ext_moneyremit_mobooking_dom;

CREATE TABLE mis_db.ext_moneyremit_mobooking_ap ON CLUSTER cluster_1S_2R
(

    `mobooking_aps_id` Int64,

    `origin_pin` Int32,

    `origin_name` String,

    `origin_office_id` Int32,

    `destination_pin` Int64,

    `destination_office_id` Int32,

    `destination_name` String,

    `mo_value` Decimal(9,
 2),

    `mo_commission` Decimal(9,
 2),

    `total_value` Decimal(9,
 2),

    `message_code` String,

    `message_content` String,

    `is_12many` Bool,

    `is_rebooking` Bool,

    `first_reference` String,

    `other_reference` String,

    `sender_name` String,

    `sender_company_name` String,

    `sender_city` String,

    `sender_state` String,

    `sender_pincode` Int32,

    `sender_email_id` String,

    `sender_alt_contact_no` String,

    `sender_kyc_reference` String,

    `sender_tax_reference` String,

    `sender_dac_id` String,

    `receiver_name` String,

    `receiver_company_name` String,

    `receiver_city` String,

    `receiver_state` String,

    `receiver_pincode` Int32,

    `receiver_email_id` String,

    `receiver_alt_contact_no` String,

    `receiver_kyc_reference` String,

    `receiver_tax_reference` String,

    `receiver_dac_id` String,

    `pick_up` Bool,

    `pickup_office_id` Int32,

    `pickup_addr_ref_sender` Int64,

    `pickup_office_name` String,

    `pickup_office_pincode` Int32,

    `pnr_no` String,

    `bkg_ref_id` String,

    `office_id_bkg` Int32,

    `ip_address_bkg` String,

    `office_id_auth_bkg` Int32,

    `ip_address_auth_bkg` String,

    `office_id_upd` Int32,

    `ip_address_upd` String,

    `office_id_upd_auth` Int32,

    `ip_address_upd_auth` String,

    `sender_mobile` Int64,

    `receiver_mobile` Int64,

    `sender_country` String,

    `receiver_country` String,

    `pg_tran_ref` String,

    `bulk_ref` String,

    `mo_type_code` String,

    `sender_address_line1` String,

    `sender_address_line2` String,

    `sender_address_line3` String,

    `receiver_address_line1` String,

    `receiver_address_line2` String,

    `receiver_address_line3` String,

    `status_code` String,

    `payment_mode_code` String,

    `md_counter_no` Int32,

    `md_shift_no` Int32,

    `md_created_date` DateTime64(6),

    `md_created_by` String,

    `md_authorised_date_bkg` DateTime64(6),

    `md_authorised_by_bkg` String,

    `md_updated_date` DateTime64(6),

    `md_updated_by` String,

    `md_updated_date_auth` DateTime64(6),

    `md_updated_by_auth` String,

    `user_type_code` String,

    `channel_type_code` String,

    `bulk_payee_ref_id` Int32,

    `bulk_customer_id` Int64,

    `contract_id` Int64,

    `cgst` Decimal(10,
 2),

    `sgst` Decimal(10,
 2),

    `utgst` Decimal(10,
 2),

    `igst` Decimal(10,
 2),

    `bkg_office_gst_no` String,

    `sender_gst_no` String,

    `receiver_gst_no` String,

    `_peerdb_synced_at` DateTime64(9) DEFAULT now64(),

    `_peerdb_is_deleted` Int8,

    `_peerdb_version` Int64
)
ENGINE = ReplicatedReplacingMergeTree(_peerdb_version)
PRIMARY KEY mobooking_aps_id
ORDER BY mobooking_aps_id
SETTINGS index_granularity = 8192;


-- mis_db.ext_moneyremit_mobooking_dom definition

CREATE TABLE mis_db.ext_moneyremit_mobooking_dom ON CLUSTER cluster_1S_2R
(

    `mobooking_dom_id` Int32,

    `origin_pin` Int32,

    `origin_name` String,

    `origin_office_id` Int32,

    `destination_pin` Int32,

    `destination_office_id` Int32,

    `destination_name` String,

    `mo_value` Decimal(9,
 2),

    `mo_commission` Decimal(9,
 2),

    `total_value` Decimal(9,
 2),

    `message_code` String,

    `message_content` String,

    `is_12many` Bool,

    `is_rebooking` Bool,

    `first_reference` String,

    `other_reference` String,

    `sender_name` String,

    `sender_company_name` String,

    `sender_city` String,

    `sender_state` String,

    `sender_pincode` Int32,

    `sender_email_id` String,

    `sender_alt_contact_no` String,

    `sender_kyc_reference` String,

    `sender_tax_reference` String,

    `sender_dac_id` String,

    `receiver_name` String,

    `receiver_company_name` String,

    `receiver_city` String,

    `receiver_state` String,

    `receiver_pincode` Int32,

    `receiver_email_id` String,

    `receiver_alt_contact_no` String,

    `receiver_kyc_reference` String,

    `receiver_tax_reference` String,

    `receiver_dac_id` String,

    `pick_up` Bool,

    `pickup_office_id` Int32,

    `pickup_addr_ref_sender` Int64,

    `pickup_office_name` String,

    `pickup_office_pincode` Int32,

    `pnr_no` String,

    `bkg_ref_id` String,

    `office_id_bkg` Int32,

    `ip_address_bkg` String,

    `office_id_auth_bkg` Int32,

    `ip_address_auth_bkg` String,

    `office_id_upd` Int32,

    `ip_address_upd` String,

    `office_id_upd_auth` Int32,

    `ip_address_upd_auth` String,

    `sender_country` String,

    `receiver_country` String,

    `sender_mobile` Int64,

    `receiver_mobile` Int64,

    `pg_tran_ref` String,

    `bulk_ref` String,

    `mo_type_code` String,

    `sender_address_line1` String,

    `sender_address_line2` String,

    `sender_address_line3` String,

    `receiver_address_line1` String,

    `receiver_address_line2` String,

    `receiver_address_line3` String,

    `status_code` String,

    `payment_mode_code` String,

    `md_counter_no` Int32,

    `md_shift_no` Int32,

    `md_created_date` DateTime64(6),

    `md_created_by` String,

    `md_authorised_date_bkg` DateTime64(6),

    `md_authorised_by_bkg` String,

    `md_updated_date` DateTime64(6),

    `md_updated_by` String,

    `md_updated_date_auth` DateTime64(6),

    `md_updated_by_auth` String,

    `user_type_code` String,

    `channel_type_code` String,

    `bulk_payee_ref_id` Int32,

    `bulk_customer_id` Int64,

    `contract_id` Int64,

    `cgst` Decimal(10,
 2),

    `sgst` Decimal(10,
 2),

    `utgst` Decimal(10,
 2),

    `igst` Decimal(10,
 2),

    `bkg_office_gst_no` String,

    `sender_gst_no` String,

    `receiver_gst_no` String,

    `_peerdb_synced_at` DateTime64(9) DEFAULT now64(),

    `_peerdb_is_deleted` Int8,

    `_peerdb_version` Int64
)
ENGINE = ReplicatedReplacingMergeTree(_peerdb_version)
PRIMARY KEY mobooking_dom_id
ORDER BY mobooking_dom_id
SETTINGS index_granularity = 8192;