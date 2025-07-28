total schedules 66851--93485
in mysore division 280(281 in training) offices are there and 8 types of offices 
BNP	1
BPC	1
BPO	205--204
HPO	2
LPC	1
PDN	1
SDO	4
SPO	65
total 34 schedules are there in mysore division
ACTIVE	24--11
INACTIVE	7--137
DELETE	3--7
select * from carriermgmt.kafka_office_hierarchy_master where division_name = 'Mysuru Division'
SELECT * 
FROM carriermgmt.kafka_office_hierarchy_master 
WHERE division_name LIKE 'Mys%';

21260000--21530030

select * from carriermgmt.kafka_office_hierarchy_master where division_office_id = '21530030' and  office_type_code='SPO'
select distinct(office_type_code) from carriermgmt.kafka_office_hierarchy_master where division_office_id = '21530030'
select count(*) from carriermgmt.kafka_office_hierarchy_master where division_office_id = '21530030'--279

SELECT office_type_code, COUNT(*) AS office_count
FROM carriermgmt.kafka_office_hierarchy_master
WHERE division_office_id = '21530030'
GROUP BY office_type_code;

select * from carriermgmt.schedule where schedule_create_office_id ='21530030' and  schedule_status='ACTIVE'
select schedule_status,count(*)  as count from carriermgmt.schedule s where s.schedule_create_office_id ='21530030' group by schedule_status
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
--training env
select * from carriermgmt.schedule_stop_sequence where schedule_id='165182'--only 3 stop
1	21661289	SERA THEKCHENLING	21661243	Bylakuppe S.O
2	21661243	Bylakuppe S.O	21661282	Periyapatna S.O
3	21661282	Periyapatna S.O	21661240	Bettadapura S.O
select * from carriermgmt.schedule_stop_sequence where schedule_id='67737'--only 3 stop
1	21661296	Vani Vilas Mohalla S.O	21661259	Jayalakshmipuram S.O 
2	21661259	Jayalakshmipuram S.O 	21661272	Manasagangothri S.O
3	21661272	Manasagangothri S.O	21460007	Mysuru NSH
select * from carriermgmt.schedule_stop_sequence where schedule_id='67692'--only 1 stop
1	21661257	Ittigegud S.O	21105973	Chamundi Betta B.O
select * from carriermgmt.schedule_stop_sequence where schedule_id='67693'--only 7 stop
1	21661289	SERA THEKCHENLING	21661243	Bylakuppe S.O
2	21661243	Bylakuppe S.O	21661282	Periyapatna S.O
3	21661282	Periyapatna S.O	21661240	Bettadapura S.O
4	21661240	Bettadapura S.O	21661286	Ravandur S.O
5	21661286	Ravandur S.O	21661264	Kattemalalavadi S.O
6	21661264	Kattemalalavadi S.O	21661255	Hunsur S.O
7	21661255	Hunsur S.O	21460007	Mysuru NSH
select * from carriermgmt.schedule_stop_sequence where schedule_id='136349'--only 2 stop
1	21661264	Kattemalalavadi S.O	21106019	Harave B.O
2	21106019	Harave B.O	21106067	Kothegala B.O
select * from carriermgmt.schedule_stop_sequence where schedule_id='136348'--only 2 stop
1	21106067	Kothegala B.O	21106019	Harave B.O
2	21106019	Harave B.O	21661264	Kattemalalavadi S.O
select * from carriermgmt.schedule_stop_sequence where schedule_id='67203'--only 6 stop
1	21630041	Mysuru Sorting  L1U	21460007	Mysuru NSH
2	21460007	Mysuru NSH	21440012	Mysuru PH
3	21440012	Mysuru PH	21661264	Kattemalalavadi S.O
4	21661264	Kattemalalavadi S.O	21661252	Hanagodu S.O
5	21661252	Hanagodu S.O	21661255	Hunsur S.O
6	21661255	Hunsur S.O	21360044	Saraswathipuram H.O
select * from carriermgmt.schedule_stop_sequence where schedule_id='67077'--only 6 stop
1	21360044	Saraswathipuram H.O	21661255	Hunsur S.O
2	21661255	Hunsur S.O	21661252	Hanagodu S.O
3	21661252	Hanagodu S.O	21661264	Kattemalalavadi S.O
4	21661264	Kattemalalavadi S.O	21460007	Mysuru NSH
5	21460007	Mysuru NSH	21440012	Mysuru PH
6	21440012	Mysuru PH	21630041	Mysuru Sorting  L1U
select * from carriermgmt.schedule_stop_sequence where schedule_id='67525'--only 2 stop
1	21106006	Gurupura B.O	21105960	Bannikuppe B.O
2	21105960	Bannikuppe B.O	21661255	Hunsur S.O
select * from carriermgmt.schedule_stop_sequence where schedule_id='67524'--only 2 stop
1	21661255	Hunsur S.O	21105960	Bannikuppe B.O
2	21105960	Bannikuppe B.O	21106006	Gurupura B.O
select * from carriermgmt.schedule_stop_sequence where schedule_id='68442'--only 1 stop
1	21360043	Mysuru H.O	21460007	Mysuru NSH

--only 23 office ids are there 
21661289, 21661243, 21661282, 21661240, 21661296, 21661259, 21661272, 21460007,
21661257, 21105973, 21661286, 21661264, 21661255, 21106019, 21106067, 21630041,
21440012, 21360044, 21360043, 21106006, 21105960

