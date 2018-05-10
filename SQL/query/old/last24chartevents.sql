SELECT
  oc.icustay_id,
  24 - ROUND((CAST(EXTRACT(EPOCH FROM og.uo_charttime1 - oc.charttime) / (60 * 60) AS NUMERIC)), 2) AS charthour,
  oc.heartrate,
  oc.sysbp,
  oc.diasbp,
  oc.meanbp,
  oc.resprate,
  oc.spo2,
  oc.tempc

FROM oliguria og
  LEFT JOIN oliguria_chartevents oc
    ON og.icustay_id = oc.icustay_id
       AND oc.charttime BETWEEN og.uo_charttime1 - INTERVAL '1' DAY AND og.uo_charttime1

