SELECT oc.icustay_id,
  ROUND( (CAST(EXTRACT(epoch FROM oc.charttime-og.intime)/(60*60) AS numeric)), 2) AS charthour,
  oc.heartrate,oc.sysbp,oc.diasbp,oc.meanbp,oc.resprate,oc.spo2,oc.tempc

FROM oliguria og
LEFT JOIN oliguria_chartevents oc
  ON og.icustay_id=oc.icustay_id
AND oc.charttime BETWEEN og.intime AND og.intime+ INTERVAL '1' DAY