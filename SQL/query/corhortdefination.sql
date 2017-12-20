DROP MATERIALIZED VIEW IF EXISTS oliguria;
CREATE MATERIALIZED VIEW oliguria AS

  --查询发生少尿事件的icustay_id，并截取第一次的时间
  WITH uo1 AS (
      SELECT DISTINCT
        icud.subject_id,
        icu.hadm_id,
        uo.icustay_id,
        --取第一次发生少尿事件的时间
        min(
            uo.charttime
        ) AS charttime
      FROM mimiciii.kdigo_uo uo

        INNER JOIN mimiciii.icustays icu
          ON icu.icustay_id = uo.icustay_id

        INNER JOIN mimiciii.icustay_detail icud
          ON icu.icustay_id = icud.icustay_id

      --入院48h后24h尿量小于400,且采集时间在出院24h之前
      WHERE uo.urineoutput_24hr < 400
            AND ROUND((CAST(EXTRACT(EPOCH FROM uo.charttime - icu.intime) / (60 * 60 * 24) AS NUMERIC)), 4) > 2
            AND ROUND((CAST(EXTRACT(EPOCH FROM icu.outtime - uo.charttime) / (60 * 60 * 24) AS NUMERIC)), 4) > 1
            AND icud.icustay_seq = 1
            AND icud.hospstay_seq = 1
            AND icud.age > 18
      GROUP BY icud.subject_id, icu.hadm_id, uo.icustay_id),

    --合并查询第一次发生少尿时间的尿量
      uo2 AS (
        SELECT
          uo1.subject_id,
          uo1.hadm_id,
          uo1.icustay_id,
          uo1.charttime,
          uo.weight,
          uo.urineoutput_24hr
        FROM uo1
          INNER JOIN public.kdigo_uo uo
            ON uo.icustay_id = uo1.icustay_id AND uo1.charttime = uo.charttime),

    --排除发生少尿24h区间内心率、收缩压出现为0的患者
      ex AS (
        SELECT
          ce.subject_id,
          ce.hadm_id,
          ce.icustay_id,
          ce.charttime,
          ce.itemid,
          ce.valuenum,
          uo2.charttime         AS uo_charttime1,
          uo2.charttime + '1 D' AS uo_charttime2,
          uo2.urineoutput_24hr
        FROM uo2
          LEFT JOIN mimiciii.chartevents ce
            ON uo2.subject_id = ce.subject_id
               AND uo2.hadm_id = ce.hadm_id
               AND uo2.icustay_id = uo2.icustay_id
        WHERE (ce.itemid IN (211, 220045) OR
               ce.itemid IN
               (
                 6 -- ABP [Systolic]
                 , 51 -- Arterial BP [Systolic]
                 , 455 -- NBP [Systolic]
                 , 6701 -- Arterial BP #2 [Systolic]
                 , 220050 -- Arterial Blood Pressure systolic
                 , 220179 -- Non Invasive Blood Pressure systolic
               ))
              AND ce.charttime >= uo2.charttime AND ce.charttime <= uo2.charttime + '1 D'
              AND ce.valuenum > 0
    ),


    --取出发生少尿事件的所有患者以及对应的肌酐测量值
      ct AS (
        SELECT
          ex.subject_id,
          ex.hadm_id,
          ex.icustay_id,
          ex.uo_charttime1,
          ex.uo_charttime2,
          ex.urineoutput_24hr,
          le.itemid AS creatinine,
          le.charttime,
          le.valuenum,
          le.flag,
          icud.intime,
          icud.outtime,
          icud.los_icu
        FROM ex
          LEFT JOIN mimiciii.labevents le
            ON ex.subject_id = le.subject_id AND ex.hadm_id = le.hadm_id
          LEFT JOIN mimiciii.icustay_detail icud
            ON ex.subject_id = icud.subject_id AND ex.hadm_id = icud.hadm_id AND ex.icustay_id = icud.icustay_id
        WHERE le.itemid = 50912
              AND le.charttime <= icud.outtime
      --AND le.charttime<icud.intime OR le.charttime>icud.outtime
    ),

    --查询入院第一次测量肌酐的时间点
      ct_first AS (
        SELECT
          ct.subject_id,
          ct.hadm_id,
          ct.icustay_id,
          min(charttime) AS first_charttime
        FROM ct
        --       WHERE le.itemid = 50912 --肌酐
        GROUP BY ct.subject_id, ct.hadm_id, ct.icustay_id),

    --第一次肌酐测量值小于4
      ct2 AS (
        SELECT ct.*
        FROM ct_first
          INNER JOIN ct
            ON ct_first.first_charttime = ct.charttime
        WHERE ct.valuenum < 4
    ),

    --查询住院期间肌酐测量次数>=2的患者
      ct_in AS (
        SELECT
          ct.subject_id,
          ct.hadm_id,
          ct.icustay_id,
          count(ct.charttime)
        FROM ct
        WHERE ct.charttime BETWEEN ct.intime AND ct.outtime
        GROUP BY ct.subject_id, ct.hadm_id, ct.icustay_id
        HAVING count(ct.charttime) >= 2
    )

  SELECT DISTINCT
    ct2.subject_id,
    ct2.hadm_id,
    ct2.icustay_id,
    ct2.uo_charttime1,
    ct2.uo_charttime2,
    ct2.urineoutput_24hr,
    ct2.intime,
    ct2.outtime
  FROM ct2
    INNER JOIN ct_in
      ON ct2.subject_id = ct_in.subject_id
         AND ct2.hadm_id = ct_in.hadm_id
         AND ct2.icustay_id = ct_in.icustay_id


