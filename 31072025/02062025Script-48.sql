RK434608493IN
select * from bagmgmt.kafka_article_transaction where article_number='RK434608493IN'
select * from bagmgmt.article_induction_stg where article_number='RK434608493IN'
-------------------03-06-2025
MP057291699IN
LakshmiPuram PO
select * from bagmgmt.kafka_article_transaction where article_number='MP057291699IN'
select * from bagmgmt.article_induction_stg where article_number='MP057291699IN'
select * from bagmgmt.bag_master

select * from bagmgmt.bag_close_content bcc where article_number='JD424821465IN'
select * from bagmgmt.bag_close_content bcc where article_number='CT249705023IN'
select * from bagmgmt.bag_close_content bcc where article_number in ('JD424821465IN', 'CK518188486IN', 'CK467769184IN', 'CK467769269IN', 'CK525713828IN', 'CK525713964IN', 'CK525713831IN', 'CK525713845IN', 'CK525713978IN', 'CK527765173IN', 'RK445662420IN', 'CK544213232IN', 'CK683975392IN', 'CK699874961IN', 'CK699875220IN', 'CK699875233IN', 'CK699875247IN', 'CK683975503IN', 'CK683975517IN', 'EK541967052IN', 'CK550511411IN', 'CK550511425IN', 'CK550511439IN', 'CK550511442IN', 'CK550511456IN', 'CK550511460IN', 'EK542003253IN', 'EK542003267IN', 'EK542003275IN', 'EK542003372IN', 'EK542003386IN', 'EK542003491IN', 'EK542003505IN', 'CK464033081IN', 'CK464033064IN', 'CK677247761IN', 'CK621898735IN', 'CK621898695IN', 'CK525182665IN', 'CK509468165IN', 'CK509468182IN', 'CK509468205IN', 'CK509468134IN', 'CK509468253IN', 'CK509468267IN', 'CK509468275IN', 'CK509468298IN', 'CK509468307IN', 'EK884519806IN', 'CK434949237IN', 'CK434949492IN', 'CK434949501IN', 'CK472688727IN', 'CK677247735IN', 'CK537881412IN', 'CL475872492IN', 'EM109989426IN', 'CK556363910IN', 'CK556364036IN', 'CK556364040IN', 'CK556364053IN', 'CK559500067IN', 'CK559500257IN', 'CK559500019IN', 'CK559500138IN', 'CK559500265IN', 'EK903144096IN', 'EK903144105IN', 'CK449110532IN', 'CK449110550IN', 'CK488843027IN', 'CK488843132IN', 'CK488843248IN', 'CK488843251IN', 'CK552947266IN', 'CK552947270IN', 'CK552947283IN', 'CK559500376IN', 'CK559500380IN', 'CK559500495IN', 'CK559500504IN', 'CK559500610IN', 'CK449110869IN', 'CK449110872IN', 'CK527764663IN', 'CK527764717IN', 'CK556363923IN', 'CK556363937IN', 'CK559469733IN', 'EK903145180IN', 'EK903145281IN', 'CK538699617IN', 'CK538699736IN', 'CK538699991IN', 'CK538700006IN', 'CK559500393IN', 'CK559500478IN', 'CK559500518IN', 'CK559500623IN', 'CK449110890IN', 'CK449111025IN', 'CK552947371IN', 'CK552947385IN', 'CK552947408IN', 'CK552947513IN', 'CK552947527IN', 'CK449110926IN', 'CK449111158IN', 'CK556364610IN', 'CK556364623IN', 'CK433437130IN', 'CK434949909IN', 'CK434949912IN', 'CK434949926IN', 'EK903145128IN', 'EK903145247IN', 'EK903145352IN', 'CK449108556IN', 'CK449111895IN', 'CK552947460IN', 'CK552947473IN', 'CK552947487IN', 'CK559500402IN', 'CK559500521IN', 'CK559500637IN', 'CK559500645IN', 'CM159588173IN', 'CP256481198IN', 'CT249705023IN'
)

select * from bagmgmt.kafka_employee_master kem where employee_id='10042090'
sele
-----
--04062025
ANALYZE bagmgmt.bag_event;
ANALYZE bagmgmt.bag_open_content;
ANALYZE bagmgmt.article_induction_stg;
ANALYZE bagmgmt.kafka_office_master;

explain (analyze,buffers) SELECT DISTINCT
    boc.bag_number AS BagNumber,
    boc.article_number AS ArticleNumber,
    boc.article_type AS ArticleType,
    ais.destn_office_pin AS DestinationPin,
    ais.booking_office_name AS BookingOffice,
    boc.insured_flag AS InsuredFlag,
    be1.user_id AS UserID,
    omt.office_name AS OfficeName,
    boc.booking_reference_id AS BookingReferenceID
FROM bagmgmt.bag_open_content boc
LEFT JOIN (
    SELECT ais1.*
    FROM bagmgmt.article_induction_stg ais1
    INNER JOIN (
        SELECT article_number, MAX(art_stg_date) AS max_date
        FROM bagmgmt.article_induction_stg
        GROUP BY article_number
    ) ais2
    ON ais1.article_number = ais2.article_number
    AND ais1.art_stg_date = ais2.max_date
) ais ON boc.article_number = ais.article_number
INNER JOIN bagmgmt.bag_event be1
    ON be1.bag_number = boc.bag_number
    AND be1.event_type IN ('OP', 'OR')
    AND be1.set_number = 'SET1'
    AND be1.set_date = '2025-06-04'
    AND be1.to_office_id = '21360043'
LEFT JOIN bagmgmt.kafka_office_master omt
    ON be1.from_office_id = omt.office_id
WHERE boc.bag_number = ANY (
    SELECT be.bag_number
    FROM bagmgmt.bag_event be
    WHERE be.set_date = '2025-06-04'
      AND be.to_office_id = '21360043'
      AND be.set_number = 'SET1'
)
AND boc.article_type <> 'BG'
AND boc.art_status <> 'S';
-----
SELECT DISTINCT boc.bag_number as BagNumber, boc.article_number as ArticleNumber, boc.article_type as ArticleType,ais.destn_office_pin as DestinationPin,ais.booking_office_name as BookingOffice,boc.insured_flag as InsuredFlag,be1.user_id as UserID,omt.office_name as OfficeName,boc.booking_reference_id AS BookingReferenceID FROM bagmgmt.bag_open_content boc LEFT JOIN (SELECT ais1.* FROM bagmgmt.article_induction_stg ais1 INNER JOIN (SELECT article_number, MAX(art_stg_date) as max_date FROM bagmgmt.article_induction_stg GROUP BY article_number) ais2 ON ais1.article_number = ais2.article_number AND ais1.art_stg_date = ais2.max_date) ais ON boc.article_number = ais.article_number JOIN bagmgmt.bag_event be1 ON be1.bag_number = boc.bag_number AND be1.event_type IN ('OP','OR') AND be1.set_number = $1 AND be1.set_date = $2 AND be1.to_office_id = $3 LEFT JOIN bagmgmt.kafka_office_master omt ON be1.from_office_id = omt.office_id WHERE boc.bag_number= ANY(SELECT be.bag_number FROM bagmgmt.bag_event be WHERE be.set_date = $4 AND be.set_number = $5 AND be.to_office_id = $6) AND boc.article_type<>'BG' AND boc.art_status NOT IN ($7)" query_name=unknown args="[SET1 2025-06-04 21360043 2025-06-04 SET1 21360043 S]
select * from bagmgmt.article_transaction 

select * from bagmgmt.article_induction_stg ais where article_number in ('EK546424906IN','EK546424910IN','EK546425005IN')
--'EK957707997IN' --10042796--570001--21250002
--10021923
select * from bagmgmt.kafka_employee_master where employee_id='10021923'
select * from bagmgmt.kafka_mailbooking_dom where article_number='EK957707997IN'


---------------------

explain (analyze,buffers) WITH max_article_date AS (
    SELECT article_number, MAX(art_stg_date) AS max_art_stg_date
    FROM bagmgmt.article_induction_stg
    GROUP BY article_number
)
SELECT DISTINCT
    boc.bag_number AS BagNumber,
    boc.article_number AS ArticleNumber,
    boc.article_type AS ArticleType,
    ais.destn_office_pin AS DestinationPin,
    ais.booking_office_name AS BookingOffice,
    boc.insured_flag AS InsuredFlag,
    be1.user_id AS UserID,
    omt.office_name AS OfficeName,
    boc.booking_reference_id AS BookingReferenceID
FROM bagmgmt.bag_open_content boc
INNER JOIN bagmgmt.bag_event be1
    ON boc.bag_number = be1.bag_number
    AND be1.event_type IN ('OP', 'OR')
    AND be1.set_number = 'SET1'
    AND be1.set_date = '2025-06-04'
    AND be1.to_office_id = '21360043'
LEFT JOIN max_article_date mad
    ON boc.article_number = mad.article_number
LEFT JOIN bagmgmt.article_induction_stg ais
    ON boc.article_number = ais.article_number
    AND ais.art_stg_date = mad.max_art_stg_date
LEFT JOIN bagmgmt.kafka_office_master omt
    ON be1.from_office_id = omt.office_id
WHERE boc.article_type <> 'BG'
  AND boc.art_status <> 'S'
ORDER BY
    boc.bag_number,
    boc.article_number,
    boc.article_type,
    ais.destn_office_pin,
    ais.booking_office_name,
    boc.insured_flag,
    be1.user_id,
    omt.office_name,
    boc.booking_reference_id;

-----05062025
cte := `
	WITH latest_article_induction AS (
		SELECT 
			article_number, 
			MAX(art_stg_date) AS latest_art_stg_date
		FROM article_induction_stg
		GROUP BY article_number
	)
`

query := psql.
	Select(
		"DISTINCT iv.article_bag_number AS ArticleBagNumber",
		"iv.art_type AS ArticleType",
		"iv.booking_office AS ClosedByOffice",
		"iv.weight",
		"iv.received_by",
		"iv.approved_status AS ApprovedStatus",
		"be_cl.bag_number AS ClosedBagNumber",
		"COALESCE(kom_cl.office_name, '') AS ClosedOfficeName",
		"be_op.bag_number AS OpenedBagNumber",
		"COALESCE(kom_op.office_name, '') AS OpenedOfficeName",
		"COALESCE(la.destn_office_pin, '999999') AS DestinationPin",
		"COALESCE(la.booking_office_name, 'UNKNOWN') AS BookingOfficeName",
		"iv.set_date AS SetDate",
		"iv.receive_approved_date AS ReceiveApprovedDate",
		"iv.close_approved_date AS ClosedApprovedDate",
		"iv.event_type AS EventType",
	).
	Prefix(cte).
	From("bagmgmt.insured_verification iv").
	LeftJoin(`
		bagmgmt.bag_close_content bcc 
		ON iv.article_bag_number = bcc.article_number
	`).
	LeftJoin(`
		bagmgmt.bag_event be_cl 
		ON bcc.bag_number = be_cl.bag_number 
		AND be_cl.event_type IN ('CL','BL','TL','DP') 
		AND be_cl.from_office_id = ?
	`, fromOfficeID).
	LeftJoin("bagmgmt.kafka_office_master kom_cl ON be_cl.to_office_id = kom_cl.office_id").
	LeftJoin(`
		bagmgmt.bag_open_content boc 
		ON iv.article_bag_number = boc.article_number
	`).
	LeftJoin(`
		bagmgmt.bag_event be_op 
		ON boc.bag_number = be_op.bag_number 
		AND be_op.event_type IN ('OP','OR') 
		AND be_op.to_office_id = ?
	`, toOfficeID).
	LeftJoin("bagmgmt.kafka_office_master kom_op ON be_op.from_office_id = kom_op.office_id").
	LeftJoin(`
		article_induction_stg la 
		ON iv.article_bag_number = la.article_number
	`).
	LeftJoin(`
		latest_article_induction lai 
		ON iv.article_bag_number = lai.article_number 
		AND la.art_stg_date = lai.latest_art_stg_date
	`).
	Where(sq.And{
		sq.Eq{"iv.office": office},
		sq.Eq{"iv.set_name": setName},
		sq.Eq{"DATE(iv.set_date)": setDate},
		sq.NotEq{"iv.art_type": "BG"},
		sq.Eq{"iv.approved_status": approveStatus},
	})

if approveStatus == "R" {
	query = query.Where(sq.Expr("iv.event_type IS DISTINCT FROM 'OP'"))
}
----------------------
cte := `
	WITH latest_article_induction AS (
		SELECT 
			article_number, 
			MAX(art_stg_date) AS latest_art_stg_date
		FROM bagmgmt.article_induction_stg
		GROUP BY article_number
	)
`

query := psql.
	Select(
		"DISTINCT iv.article_bag_number AS ArticleBagNumber",
		"iv.art_type AS ArticleType",
		"iv.booking_office AS ClosedByOffice",
		"iv.weight",
		"iv.received_by",
		"iv.approved_status AS ApprovedStatus",
		"be_cl.bag_number AS ClosedBagNumber",
		"COALESCE(kom_cl.office_name, '') AS ClosedOfficeName",
		"be_op.bag_number AS OpenedBagNumber",
		"COALESCE(kom_op.office_name, '') AS OpenedOfficeName",
		"COALESCE(la.destn_office_pin, '999999') AS DestinationPin",
		"COALESCE(la.booking_office_name, 'UNKNOWN') AS BookingOfficeName",
		"iv.set_date AS SetDate",
		"iv.receive_approved_date AS ReceiveApprovedDate",
		"iv.close_approved_date AS ClosedApprovedDate",
		"iv.event_type AS EventType",
	).
	Prefix(cte).
	From("bagmgmt.insured_verification iv").
	// Close Bag join
	LeftJoin(`
		bagmgmt.bag_close_content bcc 
		ON iv.article_bag_number = bcc.article_number
	`).
	LeftJoin(`
		bagmgmt.bag_event be_cl 
		ON bcc.bag_number = be_cl.bag_number 
		AND be_cl.event_type IN ('CL','BL','TL','DP') 
		AND be_cl.from_office_id = ?
	`, fromOfficeID).
	LeftJoin("bagmgmt.kafka_office_master kom_cl ON be_cl.to_office_id = kom_cl.office_id").
	// Open Bag join
	LeftJoin(`
		bagmgmt.bag_open_content boc 
		ON iv.article_bag_number = boc.article_number
	`).
	LeftJoin(`
		bagmgmt.bag_event be_op 
		ON boc.bag_number = be_op.bag_number 
		AND be_op.event_type IN ('OP','OR') 
		AND be_op.to_office_id = ?
	`, toOfficeID).
	LeftJoin("bagmgmt.kafka_office_master kom_op ON be_op.from_office_id = kom_op.office_id").
	// Latest article induction join
	LeftJoin(`
		bagmgmt.article_induction_stg la 
		ON iv.article_bag_number = la.article_number
	`).
	LeftJoin(`
		latest_article_induction lai 
		ON iv.article_bag_number = lai.article_number 
		AND la.art_stg_date = lai.latest_art_stg_date
	`).
	Where(sq.And{
		sq.Eq{"iv.office": office},
		sq.Eq{"iv.set_name": setName},
		sq.NotEq{"iv.art_type": "BG"},
		sq.Eq{"iv.approved_status": approveStatus},
	})

if approveStatus == "R" {
	query = query.Where(sq.Expr("iv.event_type IS DISTINCT FROM 'OP'"))
}
------------------------
SELECT boc.article_number, boc.art_status, boc.insured_flag
FROM bag_open_content boc
WHERE boc.bag_number IN (
    SELECT be.bag_number
    FROM bag_event be
    WHERE be.event_type IN ('OP', 'OR')
    AND be.to_office_id = ?
    AND be.set_number = ?
    AND be.set_date = ?
)
---
SELECT article_number, article_status as art_status, insured_flag
FROM article_induction_stg
WHERE booking_office_id = ?

select * from bagmgmt.insured_verification where article_bag_number='CBK3013464043'
-----06-06-2025

select * from bagmgmt.insured_verification where article_bag_number in ('CK431906175IN','CBK3003019097')
select * from bagmgmt.bag_open_content where bag_number='CBK3003019097'
EBX2214120011
select * from bagmgmt.bag_open_content where bag_number='EBX2214120011'
select * from bagmgmt.kafka_office_master where 

select * from bagmgmt.bag_event where bag_number='BOX0000035173'
select * from bagmgmt.bag_event where bag_number='BOX0000029667'
