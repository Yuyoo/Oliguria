DROP MATERIALIZED VIEW
IF
	EXISTS oliguria_base_cohort CASCADE;
CREATE MATERIALIZED VIEW oliguria_base_cohort AS 
-- oliguria cohort
-- los_icu >= 48h
-- age >= 18
-- first_hosp_stay and first_icu_stay
-- count(creatinine) >= 2
-- creatinine_first_value <= 4
WITH icu_base as (
SELECT * 
FROM icustay_detail icud
WHERE icud.los_icu >= 2
AND icud.age >= 18
AND icud.first_hosp_stay = 'Y'
AND icud.first_icu_stay = 'Y'
),
-- count(creatinine)>=2
crt_cnt as (
SELECT DISTINCT icustay_id
FROM icu_base
INNER JOIN labevents le
on icu_base.hadm_id = le.hadm_id
AND le.charttime BETWEEN icu_base.intime and icu_base.outtime
WHERE le.itemid = 50912
GROUP BY icu_base.icustay_id
HAVING count(le.charttime) >= 2
),
-- creatinine_first
crt_first as (
SELECT icustay_id,min(le.charttime) as firsttime
FROM icu_base
inner join labevents le
on icu_base.hadm_id = le.hadm_id
and le.charttime BETWEEN (icu_base.intime - INTERVAL '6' hour) and icu_base.outtime
WHERE le.itemid = 50912
GROUP BY icu_base.icustay_id
),
-- creatinine_first <= 4 
crt_first4 as (
SELECT icu_base.icustay_id
FROM icu_base
inner join labevents le
on icu_base.hadm_id = le.hadm_id
and le.charttime BETWEEN (icu_base.intime - INTERVAL '6' hour) and icu_base.outtime
inner join crt_first cf
on icu_base.icustay_id = cf.icustay_id
and le.charttime = cf.firsttime
WHERE le.itemid = 50912
and le.valuenum <= 4
),
-- , urine_last as (
-- SELECT uo.icustay_id
-- FROM icu_base
-- inner join kdigo_uo uo
-- icu_base.icustay_id = uo.icustay_id
-- WHERE ROUND((CAST(EXTRACT(EPOCH FROM icu_base.outtime - uo.charttime) / (60 * 60 ) AS NUMERIC)), 1) >= 24
-- )
base as (
SELECT icu_base.subject_id,icu_base.hadm_id,icu_base.icustay_id,icu_base.gender,icu_base.age,icu_base.hospital_expire_flag,icu_base.intime,icu_base.outtime,icu_base.los_icu
FROM icu_base 
inner JOIN crt_cnt cc
on icu_base.icustay_id = cc.icustay_id
inner join crt_first4 as cf4
on icu_base.icustay_id = cf4.icustay_id
-- inner join urine_last ul
-- on icu_base.icustay_id = ul.icustay_id
)
-- SELECT *
-- FROM base
-- WHERE icustay_id in (
-- SELECT icustay_id 
-- FROM base
-- except
-- SELECT icustay_id
-- FROM oliguria 
-- WHERE los_og < 24)
-- 
, cohort as (
SELECT *
FROM base
WHERE not EXISTS (
SELECT * FROM oliguria og
WHERE og.icustay_id = base.icustay_id
and og.los_og < 24
)
and
NOT EXISTS (
SELECT * FROM urineoutput uo
WHERE base.icustay_id = uo.icustay_id 
and uo."value" < 0
)
)
SELECT ch.*,
case when ch.icustay_id in (
SELECT icustay_id FROM oliguria og
WHERE og.los_og >=24)
then (og.og_starttime - interval '24' hour) 
else ch.intime
end as starttime
, og.og_starttime
, case 
WHEN ch.icustay_id in (
SELECT icustay_id FROM oliguria og
WHERE og.los_og >=24)
then 1
else 0
end as og_label
FROM cohort ch
left join oliguria og
on ch.icustay_id = og.icustay_id