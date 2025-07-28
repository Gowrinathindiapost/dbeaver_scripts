SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%lanahalli%'
--21661237 Alanahalli S.O pincode 570028 reporting office id ---21360043
select * from carriermgmt.kafka_office_hierarchy_master where division_office_id='21530030' circle_office_id='21300001'circle_name='Karnataka' office_id = '21661237'
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21530030' '21661237'
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21105947) or next_stop_office_id in (21105947)--202695,203529
select * from carriermgmt.schedule where schedule_id in ('202695' ,'203529') and schedule_status='ACTIVE'


--------------
SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%lanahalli%'
--21661237 Alanahalli S.O pincode 570028 reporting office id ---21360043
select * from carriermgmt.kafka_office_hierarchy_master where division_office_id='21530030' circle_office_id='21300001'circle_name='Karnataka' office_id = '21661237'
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21530030' '21661237'
select * from 
carriermgmt.schedule_stop_sequence where stop_office_id IN 
(select office_id from carriermgmt.kafka_office_master kom  where reporting_office_id ='21530030') or 
next_stop_office_id in (select office_id from carriermgmt.kafka_office_master kom  where reporting_office_id ='21530030')

select count(schedule_id) as no_of_schedules,source_facility_name  from carriermgmt.schedule where source_facility_id in (
select office_id from carriermgmt.kafka_office_master
    WHERE reporting_office_id in (select division_office_id from carriermgmt.kafka_office_hierarchy_master where circle_office_id='21300001'))--('21530030','16530012') )
    group by source_facility_name  order by no_of_schedules asc
    
    
select count(schedule_id) as no_of_schedules,source_facility_name  from carriermgmt.schedule where source_facility_id in (
select office_id from carriermgmt.kafka_office_master
    WHERE reporting_office_id in ('21530030') )
    group by source_facility_name  order by no_of_schedules asc
    
    select count(schedule_id) as no_of_schedules,source_facility_name,source_facility_id   from carriermgmt.schedule where source_facility_id in (
select office_id from carriermgmt.kafka_office_hierarchy_master
    WHERE division_office_id in ('21530030','16530012') )
    group by source_facility_name,source_facility_id  order by no_of_schedules asc
    
 select count(s.schedule_id) as no_of_schedules,s.source_facility_name,s.source_facility_id,om.division_office_id,om.division_name   from carriermgmt.schedule as s
 join   carriermgmt.kafka_office_hierarchy_master om on s.source_facility_id = om.office_id
 where s.source_facility_id in (
select ohm.office_id from carriermgmt.kafka_office_hierarchy_master as ohm
    WHERE ohm.division_office_id in ('21530030') )--,'16530012'
    group by s.source_facility_name,s.source_facility_id,om.division_office_id,om.division_name  order by no_of_schedules asc
    
    
    
    select office_id,office_name,division_office_id ,division_name  from carriermgmt.kafka_office_hierarchy_master 
    where office_id not in ( select 
   distinct(source_facility_id)   from carriermgmt.schedule where source_facility_id in (
select office_id from carriermgmt.kafka_office_hierarchy_master
    WHERE division_office_id = '21530030' )
    
   ) and division_office_id = '21530030' and  office_type_code in ('RMO','ICH','NSH','SPO','BPO','BPC','BNPL','HPO') 
    select * from carriermgmt.kafka_office_hierarchy_master koh where koh.office_id = '21610000' 
   SELECT
    koh.office_id,
    koh.office_name,
    koh.division_office_id,
    koh.division_name
FROM
    carriermgmt.kafka_office_hierarchy_master koh
LEFT JOIN
    carriermgmt.schedule s ON koh.office_id = s.source_facility_id
WHERE
    koh.division_office_id = '21530030'
    AND koh.office_type_code IN ('RMO', 'ICH', 'NSH', 'SPO', 'BPO', 'BPC', 'BNPL', 'HPO')
    AND s.source_facility_id IS NULL;
   SELECT table_type
FROM information_schema.tables
WHERE table_name = 'kafka_office_hierarchy_master'
  AND table_schema = 'carriermgmt';
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'kafka_office_hierarchy_master'
  AND table_schema = 'carriermgmt';

   
   WITH OfficeHierarchy AS (
    -- Select all relevant office details from the hierarchy master
    SELECT
        office_id,
        office_name,
        division_office_id,
        division_name,
        office_type_code
    FROM
        carriermgmt.kafka_office_hierarchy_master
    WHERE
        division_office_id = '21530030'
        AND office_type_code IN ('RMO', 'ICH', 'NSH', 'SPO', 'BPO', 'BPC', 'BNPL', 'HPO')
),
ScheduledFacilities AS (
    -- Identify distinct source facility IDs that are already in the schedule
    SELECT DISTINCT
        s.source_facility_id
    FROM
        carriermgmt.schedule s
    WHERE
        s.source_facility_id IN (SELECT office_id FROM OfficeHierarchy)
)
-- Final selection: Offices from the hierarchy that are not in the scheduled facilities
SELECT
    oh.office_id,
    oh.office_name,
    oh.division_office_id,
    oh.division_name
FROM
    OfficeHierarchy oh
WHERE
    oh.office_id NOT IN (SELECT source_facility_id FROM ScheduledFacilities);
   
   select office_id,office_name from carriermgmt.kafka_office_master where office_id not in ( select 
   distinct(source_facility_id)   from carriermgmt.schedule where source_facility_id in (
select office_id from carriermgmt.kafka_office_master
    WHERE reporting_office_id = '21530030' )
    
   ) and reporting_office_id = '21530030' and  office_type_code in ('RMO','ICH','NSH','SPO','BPO','BPC','BNPL','HPO') 
    
    
SELECT DISTINCT stop_office_id AS office_id
FROM carriermgmt.schedule_stop_sequence
WHERE stop_office_id IN (
    SELECT office_id
    FROM carriermgmt.kafka_office_master
    WHERE reporting_office_id = '21530030'
)
UNION
SELECT DISTINCT next_stop_office_id AS office_id
FROM carriermgmt.schedule_stop_sequence
WHERE next_stop_office_id IN (
    SELECT office_id
    FROM carriermgmt.kafka_office_master
    WHERE reporting_office_id = '21530030'
);


21106146	Vajamangala B.O	BPO	570028	21661237	1	BO21308101003
21106075	Lalithadripura B.O	BPO	570028	21661237	1	BO21308101002
21106021	Harohalli B.O	BPO	570028	21661237	1	BO21308101001
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21661237) or next_stop_office_id in (21661237)
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21106146) or next_stop_office_id in (21106146)--197406,197407
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21106075) or next_stop_office_id in (21106075)--197416,197417
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21106021) or next_stop_office_id in (21106021)--197404,197405
select * from carriermgmt.schedule where schedule_id in ('197404' ,'197405', '197416', '197406', '197407' ,'197417', '202699', '202699', '202909', '202909') and schedule_status='ACTIVE'
--197404 197405 197416 197406 197407 197417 197483 202699 202699 202909 202909
select * from carriermgmt.schedule_stop_sequence where schedule_id='197406'
---
SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%Prakash%'
--21661258	Jaya Prakash Nagar S.O	SPO	570031	21360043
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21661258'
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21661258) or next_stop_office_id in (21661258)--202695,202909
select * from carriermgmt.schedule where schedule_id in ('202695' ,'202909') and schedule_status='ACTIVE'
select * from carriermgmt.schedule_stop_sequence where schedule_id='202695'
select * from carriermgmt.schedule_stop_sequence where schedule_id='202909'

-----
SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%dayagiri%'
--21661294	Udayagiri S.O Mysuru	SPO	570019	21360043	1	PO21308130000
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21661294'
21106120	Ramanahalli B.O	BPO	570019	21661294	1	BO21308130001
21106016	Hanchya BO	BPO	570019	21661294	1	BO21308130002
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21661294) or next_stop_office_id in (21661294)--136420,197335,197377,197378,198716,197398,197399,202699,202909
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21106120) or next_stop_office_id in (21106120)--136420,197335,197377,197378
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21106016) or next_stop_office_id in (21106016)--197398,197399
select * from carriermgmt.schedule where schedule_id in ('136420' ,'197335', '197377', '197378', '197398' ,'197399','198716','202699','202909') and schedule_status='ACTIVE'
select * from carriermgmt.schedule_stop_sequence where schedule_id='136420'
select * from carriermgmt.schedule where schedule_id ='198716'
-----
SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%South%'
--21661278	Mysuru South S.O	SPO	570008	21360043	1	PO21308122000
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21661278'
21106122	Rayanakere B.O	BPO	570008	21661278	1	BO21308122007
21106004	Gujjegowdanapura B.O	BPO	570008	21661278	1	BO21308122004
21105986	Danagalli B.O	BPO	570008	21661278	1	BO21308122001
21106144	Udbur B.O	BPO	570008	21661278	1	BO21308122009
21106136	Srirampura B.O	BPO	570008	21661278	1	BO21308122008
21106086	Marballi B.O	BPO	570008	21661278	1	BO21308122006
21106007	Gurur B.O	BPO	570008	21661278	1	BO21308122010
21105995	Doora B.O	BPO	570008	21661278	1	BO21308122002
21106042	Jayapura B.O	BPO	570008	21661278	1	BO21308122005
21106002	Gopalapura B.O	BPO	570008	21661278	1	BO21308122003
--------------------------------------------------------------

SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%mipuram%'
--21661271	Lakshmipuram S.O Mysuru	SPO	570004	21360043	1	PO21308117000
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21661271'
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21661271) or next_stop_office_id in (21661271)--202695,202909,203529
select * from carriermgmt.schedule where schedule_id in ('202695' ,'202909', '203529') and schedule_status='ACTIVE'
select * from carriermgmt.schedule_stop_sequence where schedule_id='136420'
select * from carriermgmt.schedule where schedule_id ='198716'

-----------------------------
SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%Vilas%'
--21661296	Vani Vilas Mohalla S.O	SPO	570002	21360043	1	PO21308132000
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21661296'
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21661296) or next_stop_office_id in (21661296)--202695,203529
select * from carriermgmt.schedule where schedule_id in ('202695' ,'203529') and schedule_status='ACTIVE'

--------------------------------------
SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%ysur%'
--21640132	RMS Q-II Sub Division Mysuru	SDO	570001	21600003	1	SV21350002000
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21640132'

select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21600003'
--------------------------------------
SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%ysur%'
--21250002	Mysuru BNPL	BNP	571134	21530030	1	BN21350000650
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21250002'
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21250002) or next_stop_office_id in (21250002)--202695,203529
select * from carriermgmt.schedule where schedule_id in ('' ,'') and schedule_status='ACTIVE'
------------------------------------
--------------------------------------
SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%NDC%'
--21661296	Vani Vilas Mohalla S.O	SPO	570002	21360043	1	PO21308132000
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21661296'
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21661296) or next_stop_office_id in (21661296)--202695,203529
select * from carriermgmt.schedule where schedule_id in ('202695' ,'203529') and schedule_status='ACTIVE'
--------------------------------------
SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%ysur%'
--21260023	Mysuru BPC	BPC	570001	21530030	1	PC21350000650
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21260023'
21630041
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21630041'
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21630041) or next_stop_office_id in (21630041)--202695,202909,203529
select * from carriermgmt.schedule where schedule_id in ('' ,'', '') and schedule_status='ACTIVE'
select * from carriermgmt.schedule_stop_sequence where schedule_id='136420'
select * from carriermgmt.schedule where schedule_id ='198716'
------------------------------------
SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%ysur%'
--21630041	Mysuru Sorting  RMO	570001	21600003	1	MO21350000553
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21630041'
21630041
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21630041'
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21630041) or next_stop_office_id in (21630041)--38,20372,155295,172326,197483,197617,198682,198716,
--198748,198768,199050,199062,199134,202691,202692,202694,202695,202696,202697,202698,202699,202700,202701,202702,202703,202704,202705,202706,202707,202904,202909
--203394,203400,203511,203529
select * from carriermgmt.schedule where schedule_id in ('38', '20372', '155295', '172326', '197483', '197617', '198682', '198716', '198748', '198768', '199050', '199062', '199134', '202691', '202692', '202694', '202695', '202696', '202697', '202698', '202699', '202700', '202701', '202702', '202703', '202704', '202705', '202706', '202707', '202904', '202909', '203394', '203400', '203511', '203529') and schedule_status='ACTIVE'
select * from carriermgmt.schedule_stop_sequence where schedule_id='136420'
select * from carriermgmt.schedule where schedule_id ='198716'
------------------------------------------------

SELECT * 
FROM carriermgmt.kafka_office_master kom 
WHERE kom.office_name LIKE '%ysuru%'
--21360043	Mysuru H.O	HPO	570001	21530030	1	HO21308100000
select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21360043'
21661238	Bannimantap S.O	SPO	570015	21360043	1	PO21308102000
21661294	Udayagiri S.O Mysuru	SPO	570019	21360043	1	PO21308130000
21661278	Mysuru South S.O	SPO	570008	21360043	1	PO21308122000
21661237	Alanahalli S.O	SPO	570028	21360043	1	PO21308101000
21661268	Krishnamurthypuram S.O	SPO	570004	21360043	1	PO21308116000
21661281	Note Mudran Nagar S.O	SPO	570003	21360043	1	PO21308125000
21661280	Narasimha Raja Mohalla S.O	SPO	570007	21360043	1	PO21308124000
21661296	Vani Vilas Mohalla S.O	SPO	570002	21360043	1	PO21308132000
21661247	G S Ashram S.O	SPO	570025	21360043	1	PO21308107000
21661273	Mandimohalla S.O	SPO	570001	21360043	1	PO21308119000
21661267	Krishna Rajendra Circle S.O	SPO	570001	21360043	1	PO21308115000
21661274	Metagalli S.O	SPO	570016	21360043	1	PO21308120000
21661279	Mysuru University S.O	SPO	570005	21360043	1	PO21308123000
21661272	Manasagangothri S.O	SPO	570006	21360043	1	PO21308118000
21661300	Yadavagiri S.O	SPO	570020	21360043	1	PO21308135000
21661244	Chamundi Extension S.O	SPO	570004	21360043	1	PO21308104000
21661271	Lakshmipuram S.O Mysuru	SPO	570004	21360043	1	PO21308117000
21661258	Jaya Prakash Nagar S.O	SPO	570031	21360043	1	PO21308111000
21661265	Krishna Raja Mohalla S.O	SPO	570004	21360043	1	PO21308114000
21661297	Vijayanagar III Stage S.O	SPO	571122	21360043	1	PO21308134000
21661283	PTC Campus S.O	SPO	570010	21360043	1	PO21308126000
21661291	Shivaraathreeswar Nagar S.O	SPO	570015	21360043	1	PO21308128000
21661253	Hebbal Layout S.O	SPO	570016	21360043	1	PO21308109000
21661259	Jayalakshmipuram S.O 	SPO	570012	21360043	1	PO21308112000
21661299	Vijaynagar II Stage S.O	SPO	570031	21360043	1	PO21308133000
21661276	Mysuru Fort S.O	SPO	570004	21360043	1	PO21308121000
21661292	Siddarthanagar Nagar S.O	SPO	570011	21360043	1	PO21308129000
21661249	Gokulam S.O	SPO	570002	21360043	1	PO21308108000
21661290	Shakthinagar S.O	SPO	570029	21360043	1	PO21308127000
21661257	Ittigegud S.O	SPO	570010	21360043	1	PO21308110000
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21360043) or next_stop_office_id in (21360043)
select * from carriermgmt.schedule where schedule_id in (21360043) 
select * from carriermgmt.schedule where schedule_id in ('211153' ,'211542','202693','202696',
'202904','203529','203592','204811','209465','209615'
) and schedule_status='ACTIVE'
select * from carriermgmt.kafka_office_master kom  where office_id='21530030' reporting_office_id ='21530030'
211153
211542
202693
202696
202904
203529
203529
203592
203592
204811
204811
209465
209615
---------------------------------------------------------------------------------------

21680008	Mysuru Bus Stand TMO	TMO	570001
21440012	Mysuru PH	NPH	570001
21400015	LPC Mysuru	LPC	570001
21460007	Mysuru NSH	NSH	570001

select * from carriermgmt.kafka_office_master kom  where reporting_office_id ='21460007'
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21680008) or next_stop_office_id in (21680008)
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21440012) or next_stop_office_id in (21440012)
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21400015) or next_stop_office_id in (21400015)
select * from carriermgmt.schedule_stop_sequence where stop_office_id in (21460007) or next_stop_office_id in (21460007)

select s.schedule_id,s.schedule_name,s.schedule_type,s.bag_type,s.source_facility_id ,s.source_facility_name ,s.destination_facility_id ,s.destination_facility_name ,
s.schedule_start_time,s.schedule_create_office_id,s.schedule_valid_from,s.schedule_valid_to,s.transport_mode,s.schedule_running_days,
s.schedule_status,s.created_by,s.created_date,s.updated_by,s.updated_date
from carriermgmt.schedule as s
join carriermgmt.schedule_stop_sequence sss on sss.schedule_id=s.schedule_id 
where sss.stop_office_id in (21680008) or sss.next_stop_office_id in (21680008)
and s.schedule_status='ACTIVE'

select * from carriermgmt.schedule_stop_sequence as s
join carriermgmt.schedule sss on sss.schedule_id=s.schedule_id 
where s.stop_office_id in (21680008) or s.next_stop_office_id in (21680008)
and sss.schedule_status='ACTIVE'


SELECT
    s.schedule_id,
    s.schedule_name,
    s.schedule_type,
    s.bag_type,
    s.source_facility_id,
    s.source_facility_name,
    s.destination_facility_id,
    s.destination_facility_name,
    s.schedule_start_time,
    s.schedule_create_office_id,
    s.schedule_valid_from,
    s.schedule_valid_to,
    s.transport_mode,
    s.schedule_running_days,
    s.schedule_status,
    s.created_by,
    s.created_date,
    s.updated_by,
    s.updated_date
FROM
    carriermgmt.schedule AS s
JOIN
    carriermgmt.schedule_stop_sequence sss ON sss.schedule_id = s.schedule_id
WHERE
    (sss.stop_office_id = ($1) 
    OR sss.next_stop_office_id = ($1))
AND
    s.schedule_status = 'ACTIVE';



SELECT 
    COUNT(s.schedule_id) AS no_of_schedules, 
    s.source_facility_name, 
    s.source_facility_id, 
    om.division_office_id, 
    om.division_name
FROM 
    carriermgmt.schedule AS s
JOIN 
    carriermgmt.kafka_office_hierarchy_master AS om 
    ON s.source_facility_id = om.office_id
WHERE 
    s.source_facility_id IN ($1)
GROUP BY 
    s.source_facility_name, 
    s.source_facility_id, 
    om.division_office_id, 
    om.division_name
ORDER BY 
    no_of_schedules ASC
LIMIT 
    2147483647 OFFSET 0;

