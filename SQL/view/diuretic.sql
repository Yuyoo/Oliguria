-- This query select patients using diuretic drugs
-- Three tables are used: inputevents_cv, inputevents_mv and prescriptions
-- If a patient's dose was above a value, we will label him as diuretic resis temporarily
-- The criteria is:
--          1) furosemide > 80  unit:IV   or
--          2) furosemide > 160  unit:PO   or
--          3) torasemide > 40   or
--          4) bumetanide > 2   or
--          5) ethacrynic acid > 100   or
--          6) lasix > 80  unit:IV   or
--          7) lasix > 160  unit:PO   or
--          8) torsemide > 40

DROP MATERIALIZED VIEW IF EXISTS diuretic_resis_raw CASCADE;
CREATE MATERIALIZED VIEW diuretic_resis_raw AS
WITH mv_drug AS(
SELECT subject_id
       , hadm_id
       , CASE WHEN itemid in (228340, 221794) THEN 'furosemide' END AS drug
       , starttime AS startdate
       , amount AS dose
       , amountuom AS dose_unit_rx
       , ('IV'::TEXT) AS route
  FROM inputevents_mv
 WHERE itemid IN (228340, 221794)
 ORDER BY subject_id, hadm_id
),
cv_drug AS(
SELECT subject_id
       , hadm_id
       , CASE WHEN itemid IN (45275, 46690) THEN 'bumetanide'
              WHEN itemid IN (30123) THEN 'furosemide' END AS drug
       , charttime AS startdate
       , amount AS dose
       , amountuom AS dose_unit_rx
       , ('IV'::TEXT)  AS route
  FROM inputevents_cv
 WHERE itemid IN (30123, 45275, 46690)
 ORDER BY subject_id, hadm_id
),
pr_drug AS (
    SELECT
      pr.subject_id,
      pr.hadm_id,
      CASE WHEN (lower((pr.drug)::text) ~~ '%furosemide%'::text) THEN 'furosemide'
                 WHEN (lower((pr.drug)::text) ~~ '%torasemide%'::text) THEN 'torasemide'
                 WHEN (lower((pr.drug)::text) ~~ '%torsemide%'::text) THEN 'torasemide'
                 WHEN (lower((pr.drug)::text) ~~ '%bumetanide%'::text) THEN 'bumetanide'
                 WHEN (lower((pr.drug)::text) ~~ '%ethacrynic acid%'::text) THEN 'ethacrynic acid'
                 WHEN (lower((pr.drug)::text) ~~ '%lasix%'::text) THEN 'lasix' ELSE NULL END AS drug,
      pr.startdate,
      pr.dose_val_rx::NUMERIC AS dose,
      pr.dose_unit_rx,
      CASE
      WHEN lower(pr.route) LIKE '%iv%'
        THEN 'IV'
      WHEN lower(pr.route) LIKE '%po%'
        THEN 'PO'
      WHEN lower(pr.route) LIKE '%ng%'
        THEN 'PO'
      WHEN lower(pr.route) LIKE '%pb%'
        THEN 'PO'
      WHEN lower(pr.route) LIKE '%g tube%'
        THEN 'G TUBE' END AS route
    FROM prescriptions pr
  WHERE ((lower((pr.drug)::text) ~~ '%furosemide%'::text)
              OR (lower((pr.drug)::text) ~~ '%torasemide%'::text)
              OR (lower((pr.drug)::text) ~~ '%bumetanide%'::text)
              OR (lower((pr.drug)::text) ~~ '%ethacrynic acid%'::text)
              OR (lower((pr.drug)::text) ~~ '%lasix%'::text)
              OR (lower((pr.drug)::text) ~~ '%torsemide%'::text))
             -- move serveral unwilling records
             and ((lower((pr.dose_val_rx)::text) !~~ '%\-%'::text) AND (lower((pr.dose_val_rx)::text) !~~ '%j20%'::text))
 )
, diur_drug1 AS (
SELECT * FROM mv_drug
       UNION ALL SELECT * FROM cv_drug cv WHERE dose IS NOT NULL
       UNION ALL SELECT * FROM pr_drug
 ORDER BY subject_id
         , hadm_id
)

-- add everyday's drug dose
, diur_daily AS(
  SELECT did.subject_id, did.hadm_id, did.drug, date(did.startdate) AS startdate,
         sum(did.dose) AS dose, did.dose_unit_rx, did.route
    FROM diur_drug1 did
   GROUP BY did.subject_id, did.hadm_id, did.drug, date(did.startdate), did.dose_unit_rx, did.route
)

, diur_drug2 AS (
SELECT dida.*,
       date(dida.startdate) AS date,
       --row_number() over(partition by hadm_id order by subject_id, hadm_id, startdate), -- rank drug using time
       CASE when (((lower((dida.drug)::text) ~~ '%furosemide%'::text) AND (dida.dose::NUMERIC > 80) AND (lower(dida.route) LIKE '%iv%'))) THEN 1
            when (((lower((dida.drug)::text) ~~ '%furosemide%'::text) AND (dida.dose::NUMERIC > 160) AND (lower(dida.route) LIKE '%po%'))) THEN 1
            when ((lower(dida.route) LIKE '%g tube%')) THEN 1  -- patients with this route is considered critical
            when ((lower((dida.drug)::text) ~~ '%torasemide%'::text) AND (dida.dose::NUMERIC > 40)) THEN 1
            when ((lower((dida.drug)::text) ~~ '%bumetanide%'::text) AND (dida.dose::NUMERIC > 2)) THEN 1
            when ((lower((dida.drug)::text) ~~ '%ethacrynic acid%'::text) AND (dida.dose::NUMERIC > 100)) THEN 1
            when (((lower((dida.drug)::text) ~~ '%lasix%'::text) AND (dida.dose::NUMERIC > 80) AND (dida.route LIKE '%iv%'))) THEN 1
            when (((lower((dida.drug)::text) ~~ '%lasix%'::text) AND (dida.dose::NUMERIC > 160) AND (dida.route LIKE '%po%'))) THEN 1
            when (((lower((dida.drug)::text) ~~ '%torsemide%'::text) AND (dida.dose::NUMERIC > 40))) THEN 1 END AS diur_resis
  FROM diur_daily dida
)

SELECT *
       , row_number() over(partition by hadm_id order by subject_id, hadm_id, startdate) AS druguse_seq-- rank drug using time
       , CASE WHEN d2.drug = 'furosemide' AND d2.route = 'IV' THEN d2.dose/40
                  WHEN d2.drug = 'furosemide' AND d2.route = 'PO' THEN d2.dose/80
                  WHEN d2.drug = 'torasemide' THEN d2.dose/20
                  WHEN d2.drug = 'bumetanide'  THEN d2.dose/2
                  WHEN d2.drug = 'ethacrynic acid'  THEN d2.dose/50 END AS unit
  FROM diur_drug2 d2
 ORDER BY subject_id, hadm_id
