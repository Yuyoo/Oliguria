DROP MATERIALIZED VIEW
IF
	EXISTS oliguria_chartevents CASCADE;
CREATE MATERIALIZED VIEW oliguria_chartevents AS
WITH base as (
SELECT obc.*,
case when obc.icustay_id in (
SELECT icustay_id FROM oliguria og
WHERE og.los_og >=24)
then (og.og_starttime - interval '24' hour) 
else obc.intime
end as starttime
, og.og_starttime
, case 
WHEN obc.icustay_id in (
SELECT icustay_id FROM oliguria og
WHERE og.los_og >=24)
then 1
else 0
end as og_label
FROM oliguria_base_cohort obc
left join oliguria og
on obc.icustay_id = og.icustay_id
)
SELECT base.subject_id,base.hadm_id,base.icustay_id,base.og_label,base.starttime,base.og_starttime,ce.charttime,
    max(CASE
        WHEN itemid IN (211, 220045) AND valuenum > 0 AND valuenum < 300
          THEN valuenum
        ELSE NULL END) AS HeartRate,
    -- HeartRate
    max(CASE
        WHEN itemid IN (51, 442, 455, 6701, 220179, 220050) AND valuenum > 0 AND valuenum < 400
          THEN valuenum
        ELSE NULL END) AS SysBP,
    -- SysBP 收缩压
    max(CASE
        WHEN itemid IN (8368, 8440, 8441, 8555, 220180, 220051) AND valuenum > 0 AND valuenum < 300
          THEN valuenum
        ELSE NULL END) AS DiasBP,
    -- DiasBP 扩张压
    max(CASE
        WHEN itemid IN (456, 52, 6702, 443, 220052, 220181, 225312) AND valuenum > 0 AND valuenum < 300
          THEN valuenum
        ELSE NULL END) AS MeanBP,
    -- MeanBP 平均动脉压
    max(CASE
        WHEN itemid IN (615, 618, 220210, 224690) AND valuenum > 0 AND valuenum < 70
          THEN valuenum
        ELSE NULL END) AS RespRate,
    -- RespRate
    max(CASE
        WHEN itemid IN (223761, 678) AND valuenum > 70 AND valuenum < 120
          THEN (valuenum - 32) / 1.8 -- TempF, converted to degC in valuenum call
        WHEN itemid IN (223762, 676) AND valuenum > 10 AND valuenum < 50
          THEN valuenum -- TempC
        ELSE NULL END) AS TempC,
    max(CASE
        WHEN itemid IN (646, 220277) AND valuenum > 0 AND valuenum <= 100
          THEN valuenum
        ELSE NULL END) AS SpO2
FROM base 
left join chartevents ce
on base.icustay_id = ce.icustay_id
and ce.charttime BETWEEN base.starttime and base.og_starttime
and ce.error IS DISTINCT FROM 1
where ce.itemid in
  (
  -- HEART RATE
  211, -- "Heart Rate"
  220045, -- "Heart Rate"

  -- Systolic/diastolic

  51, -- Arterial BP [Systolic]
  442, -- Manual BP [Systolic]
  455, -- NBP [Systolic]
  6701, -- Arterial BP #2 [Systolic]
  220179, -- Non Invasive Blood Pressure systolic
  220050, -- Arterial Blood Pressure systolic

  8368, -- Arterial BP [Diastolic]
  8440, -- Manual BP [Diastolic]
  8441, -- NBP [Diastolic]
  8555, -- Arterial BP #2 [Diastolic]
  220180, -- Non Invasive Blood Pressure diastolic
  220051, -- Arterial Blood Pressure diastolic


  -- MEAN ARTERIAL PRESSURE
  456, -- "NBP Mean"
  52, -- "Arterial BP Mean"
  6702, -- Arterial BP Mean #2
  443, -- Manual BP Mean(calc)
  220052, -- "Arterial Blood Pressure mean"
  220181, -- "Non Invasive Blood Pressure mean"
  225312, -- "ART BP mean"

  -- RESPIRATORY RATE
  618,-- Respiratory Rate
  615,-- Resp Rate (Total)
  220210,-- Respiratory Rate
  224690, -- Respiratory Rate (Total)


  -- SPO2, peripheral
  646, 220277,

--   -- GLUCOSE, both lab and fingerstick
--   807,-- Fingerstick Glucose
--   811,-- Glucose (70-105)
--   1529,-- Glucose
--   3745,-- BloodGlucose
--   3744,-- Blood Glucose
--   225664,-- Glucose finger stick
--   220621,-- Glucose (serum)
--   226537,-- Glucose (whole blood)

  -- TEMPERATURE
  223762, -- "Temperature Celsius"
  676,	-- "Temperature C"
  223761, -- "Temperature Fahrenheit"
  678 -- "Temperature F"

  )
	GROUP BY base.subject_id,base.hadm_id,base.icustay_id,base.starttime,ce.charttime,base.og_label,base.og_starttime