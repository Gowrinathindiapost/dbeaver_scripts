total schedules 66851
in mysore division 280 offices are there and 8 types of offices 
BNP	1
BPC	1
BPO	205
HPO	2
LPC	1
PDN	1
SDO	4
SPO	65
total 34 schedules are there in mysore division
ACTIVE	24
INACTIVE	7
DELETE	3
select * from carriermgmt.kafka_office_hierarchy_master where division_name = 'Mysore Division'
SELECT * 
FROM carriermgmt.kafka_office_hierarchy_master 
WHERE division_name LIKE 'Mys%';

21260000

select * from carriermgmt.kafka_office_hierarchy_master where division_office_id = '21260000' and  office_type_code='SPO'
select distinct(office_type_code) from carriermgmt.kafka_office_hierarchy_master where division_office_id = '21260000'
select count(*) from carriermgmt.kafka_office_hierarchy_master where division_office_id = '21260000'

SELECT office_type_code, COUNT(*) AS office_count
FROM carriermgmt.kafka_office_hierarchy_master
WHERE division_office_id = '21260000'
GROUP BY office_type_code;

select * from carriermgmt.schedule where schedule_create_office_id ='21260000' and  schedule_status='ACTIVE'
select schedule_status,count(*)  as count from carriermgmt.schedule s where s.schedule_create_office_id ='21260000' group by schedule_status
select * from carriermgmt.schedule_stop_sequence where schedule_id='66701'--only one stop
select * from carriermgmt.schedule_stop_sequence where schedule_id='66715'--only one stop
select * from carriermgmt.schedule_stop_sequence where schedule_id='66706'--only one stop
select * from carriermgmt.schedule_stop_sequence where schedule_id='66716'--only one stop
select * from carriermgmt.schedule_stop_sequence where schedule_id='66703'--only one stop
select * from carriermgmt.schedule_stop_sequence where schedule_id='66708'--only one stop
select * from carriermgmt.schedule_stop_sequence where schedule_id='66718'--only one stop
select * from carriermgmt.schedule_stop_sequence where schedule_id='66719'--only one stop
select * from carriermgmt.schedule_stop_sequence where schedule_id='66751'--only one stop
select * from carriermgmt.schedule_stop_sequence where schedule_id='66759'--only two stop21260551Mysuru H.O/21240552Srirangapatna H.O/2.21240551Mandya H.O
select * from carriermgmt.schedule_stop_sequence where schedule_id='66786'--only one stop 92000341771FPO to 92000328mail hall 
select * from carriermgmt.schedule_stop_sequence where schedule_id='66787'--only one stop  92000328mail hall to 92000341771FPO
select * from carriermgmt.schedule_stop_sequence where schedule_id='66794'--only one stop  92000020 1CBPO/92000328mail hall 
select * from carriermgmt.schedule_stop_sequence where schedule_id='66793'--only one stop  92000328mail hall/92000020 1CBPO
select * from carriermgmt.schedule_stop_sequence where schedule_id='66792'--only one stop  92000022 2CBPO/92000328mail hall 
select * from carriermgmt.schedule_stop_sequence where schedule_id='66790'--only one stop  92000247 Maill Hall (81)/Maill Hall (81) 1718 FPO
select * from carriermgmt.schedule_stop_sequence where schedule_id='66788'--only one stop  Maill Hall (81) 1718 FPO/92000247 Maill Hall (81)
select * from carriermgmt.schedule_stop_sequence where schedule_id='66791'--only one stop  92000328mail hall to 92000022 2 CBPO
select * from carriermgmt.schedule_stop_sequence where schedule_id='66798'--only one stop  92000022 2 CBPO to 92000020 1 CBPO
select * from carriermgmt.schedule_stop_sequence where schedule_id='66797'--only one stop   92000020 1 CBPO/92000022 2 CBPO
select * from carriermgmt.schedule_stop_sequence where schedule_id='66825'--only 5 stop
21890011	Mysuru PH	21260551	Mysuru H.O
21260551	Mysuru H.O	21260721	Ittigegud S.O
21260721	Ittigegud S.O	21260552	Saraswathipuram H.O
21260552	Saraswathipuram H.O	66000011	Mysuru Campus
66000011	Mysuru Campus	21890011	Mysuru PH
select * from carriermgmt.schedule_stop_sequence where schedule_id='66826'--only 5 stop
30614001	Hyderabad TMO	30624006	TMO MAHABUBNAGAR
30624006	TMO MAHABUBNAGAR	11614005	Kurnool TMO
11614005	Kurnool TMO	11614001	Anantapur TMO
11614001	Anantapur TMO	21614002	Bengaluru City TMO
21614002	Bengaluru City TMO	21631016	Mysuru Sorting  L1U
select * from carriermgmt.schedule_stop_sequence where schedule_id='66833'--only 2 stop
21260721	Ittigegud S.O	21860009	Mysuru NSH
21860009	Mysuru NSH	21260551	Mysuru H.O
select * from carriermgmt.schedule_stop_sequence where schedule_id='66839'--only 1 stop

