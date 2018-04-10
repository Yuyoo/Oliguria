-- oliguria cohort definition
-- Los_icu >= 48 hour
-- age >= 18
-- first hospital stay and first icu stay
-- count(creatinine) >=2
-- creatinine baseline <= 4
-- urine output < 400ml/day or urine output < 17ml/hour(in 12 hours)
-- exclude someone whose urine_output < 0   (beacause of GU irrigant)
-- exclude someone who has cardiac arrest during oliguria   (heartrate=0 or systolic=0) 
DROP MATERIALIZED VIEW
IF
	EXISTS oliguria CASCADE;
CREATE MATERIALIZED VIEW oliguria AS 
WITH icu_base AS (
SELECT
	* 
FROM
	icustay_detail icud 
WHERE
	icud.los_icu >= 2 
	AND icud.age >= 18 
	AND icud.first_hosp_stay = 'Y' 
	AND icud.first_icu_stay = 'Y' 
	),	-- count(creatinine)>=2
	crt_cnt AS (
SELECT DISTINCT
	icustay_id 
FROM
	icu_base
	INNER JOIN labevents le ON icu_base.hadm_id = le.hadm_id 
	AND le.charttime BETWEEN icu_base.intime 
	AND icu_base.outtime 
WHERE
	le.itemid = 50912 
GROUP BY
	icu_base.icustay_id 
HAVING
	count( le.charttime ) >= 2 
	),-- creatinine_first
	crt_first AS (
SELECT
	icustay_id,
	min( le.charttime ) AS firsttime 
FROM
	icu_base
	INNER JOIN labevents le ON icu_base.hadm_id = le.hadm_id 
	AND le.charttime BETWEEN ( icu_base.intime - INTERVAL '6' HOUR ) 
	AND icu_base.outtime 
WHERE
	le.itemid = 50912 
GROUP BY
	icu_base.icustay_id 
	),-- creatinine_first <= 4
	crt_first4 AS (
SELECT
	icu_base.icustay_id 
FROM
	icu_base
	INNER JOIN labevents le ON icu_base.hadm_id = le.hadm_id 
	AND le.charttime BETWEEN ( icu_base.intime - INTERVAL '6' HOUR ) 
	AND icu_base.outtime
	INNER JOIN crt_first cf ON icu_base.icustay_id = cf.icustay_id 
	AND le.charttime = cf.firsttime 
WHERE
	le.itemid = 50912 
	AND le.valuenum <= 4 
	),
	base_cohort AS (
SELECT
	icu_base.subject_id,
	icu_base.hadm_id,
	icu_base.icustay_id,
	icu_base.gender,
	icu_base.age,
	icu_base.hospital_expire_flag,
	icu_base.intime,
	icu_base.outtime,
	icu_base.los_icu 
FROM
	icu_base
	INNER JOIN crt_cnt cc ON icu_base.icustay_id = cc.icustay_id
	INNER JOIN crt_first4 AS cf4 ON icu_base.icustay_id = cf4.icustay_id -- inner join urine_last ul
-- on icu_base.icustay_id = ul.icustay_id
	
	) -- 查询发生少尿事件的icustay_id，并截取第一次的时间
	,
	uo1 AS (
	SELECT DISTINCT
		uo.icustay_id,-- 取第一次发生少尿事件的时间
		bc.intime,
		min( uo.charttime ) AS charttime 
	FROM
		kdigo_uo uo
		INNER JOIN base_cohort bc ON uo.icustay_id = bc.icustay_id -- 入院24h后24h尿量小于400,且采集时间在出院24h之前
		
	WHERE
		(uo.urineoutput_24hr < 400 or uo.urineoutput_12hr < 17*12)
-- 		AND ROUND(
-- 			( CAST( EXTRACT( EPOCH FROM uo.charttime - bc.intime ) / ( 60 * 60 * 24 ) AS NUMERIC ) ),
-- 			4 
-- 		) >= 1 
		AND ROUND(
			( CAST( EXTRACT( EPOCH FROM bc.outtime - uo.charttime ) / ( 60 * 60 * 24 ) AS NUMERIC ) ),
			4 
		) >= 1 
	GROUP BY
		uo.icustay_id,bc.intime
	-- HAVING ( CAST( EXTRACT( EPOCH FROM min( uo.charttime ) - bc.intime ) / ( 60 * 60 * 24 ) AS NUMERIC ) )  >= 1 
	) -- 合并查询第一次发生少尿时间的尿量
	,
	uo2 AS (
	SELECT
		uo1.icustay_id,
		uo1.charttime,
		uo1.intime,
		uo.weight,
		uo.urineoutput_24hr,
		uo.urineoutput_12hr/12 as urineoutput_h
	FROM
		uo1
		INNER JOIN kdigo_uo uo ON uo.icustay_id = uo1.icustay_id 
		AND uo1.charttime = uo.charttime 
	) 
	,
	-- 排除发生少尿24h区间内心率、收缩压出现为0的患者
	oliguria AS (
	SELECT DISTINCT
		ce.icustay_id,
		uo2.intime,
		uo2.charttime AS og_starttime,
		round((cast(EXTRACT(epoch FROM uo2.charttime - uo2.intime)/(60*60) as NUMERIC)),0) AS los_og,
		uo2.urineoutput_24hr,
		uo2.urineoutput_h
	FROM
		uo2
		LEFT JOIN chartevents ce ON uo2.icustay_id = ce.icustay_id 
	WHERE
		(
			ce.itemid IN ( 211, 220045 ) 
			OR ce.itemid IN ( 6 -- ABP [Systolic]
				, 51 -- Arterial BP [Systolic]
				, 455 -- NBP [Systolic]
				, 6701 -- Arterial BP #2 [Systolic]
				, 220050 -- Arterial Blood Pressure systolic
				, 220179 -- Non Invasive Blood Pressure systolic
			) 
		) 
		AND ce.charttime >= uo2.charttime 
		AND ce.charttime <= uo2.charttime + '1 D' AND ce.valuenum > 0 
	) 
-- 排除尿量<0
SELECT
	* 
FROM
oliguria og
WHERE NOT EXISTS (
SELECT * FROM urineoutput uo
WHERE og.icustay_id = uo.icustay_id 
and uo."value" < 0
)