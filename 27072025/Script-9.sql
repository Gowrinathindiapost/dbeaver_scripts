INSERT INTO carriermgmt.driver (driving_id, driving_license_number, driver_first_name, 
driver_last_name, driver_type, driving_certifications, aadhaar_number, driver_phone_number,
date_of_birth, contractor_id, employee_id, driver_status, 
driver_onboarding_office_id, created_by, created_date, updated_by, 
updated_date, deleted_by, deleted_date, vehicle_type, driving_license_validity_date) 
VALUES(nextval('carriermgmt.driver_driving_id_seq'::regclass), '', 'B', 
'Amitabh', 'Contractor', 
'CertACertB', '12345678911108', 1000000001, 
'1990-01-15', 1, 0, 'ACTIVE', 21960001, 'xxxx', '2025-01-28 15:54:13.932', '', '', '', '', 'LTL', '2099-12-31');
775		B	Amitabh	Contractor	CertACertB	12345678911108	1000000001	
1990-01-15	1	0	ACTIVE	21960001	xxxx	2025-01-28 15:54:13.932					LTL	2099-12-31



INSERT INTO carriermgmt.driver (
    driving_id, driving_license_number, driver_first_name, 
    driver_last_name, driver_type, driving_certifications, aadhaar_number, driver_phone_number,
    date_of_birth, contractor_id, employee_id, driver_status, 
    driver_onboarding_office_id, created_by, created_date, updated_by, 
    updated_date, deleted_by, deleted_date, vehicle_type, driving_license_validity_date
) 
VALUES (
    nextval('carriermgmt.driver_driving_id_seq'::regclass), '', 'B', 
    'Amitabh', 'Contractor', 
    'CertACertB', '12345678911108', 1000000001, 
    '1990-01-15', 1, 0, 'ACTIVE', 21960001, 'xxxx', '2025-01-28 15:54:13.932', 
    NULL, NULL, NULL, NULL, 'LTL', '2099-12-31'
);
