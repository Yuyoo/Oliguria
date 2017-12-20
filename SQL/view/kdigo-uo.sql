DROP MATERIALIZED VIEW IF EXISTS kdigo_uo;
CREATE MATERIALIZED VIEW kdigo_uo AS
  WITH ur_stg AS
  (
      SELECT
        io.icustay_id,
        io.charttime

        -- three sums:
        -- 1) over a 6 hour period
        -- 2) over a 12 hour period
        -- 3) over a 24 hour period
        ,
        sum(CASE WHEN iosum.charttime <= io.charttime + INTERVAL '5' HOUR
          THEN iosum.VALUE
            ELSE NULL END) AS UrineOutput_6hr,
        sum(CASE WHEN iosum.charttime <= io.charttime + INTERVAL '11' HOUR
          THEN iosum.VALUE
            ELSE NULL END) AS UrineOutput_12hr,
        sum(iosum.VALUE)   AS UrineOutput_24hr
      FROM urineoutput io
        -- this join gives you all UO measurements over a 24 hour period
        LEFT JOIN urineoutput iosum
          ON io.icustay_id = iosum.icustay_id
             AND iosum.charttime >= io.charttime
             AND iosum.charttime <= (io.charttime + INTERVAL '23' HOUR)
      GROUP BY io.icustay_id, io.charttime
  )
  SELECT
    ur.icustay_id,
    ur.charttime,
    wd.weight,
    ur.UrineOutput_6hr,
    ur.UrineOutput_12hr,
    ur.UrineOutput_24hr
  FROM ur_stg ur
    LEFT JOIN weightdurations wd
      ON ur.icustay_id = wd.icustay_id
         AND ur.charttime >= wd.starttime
         AND ur.charttime < wd.endtime
  ORDER BY icustay_id, charttime;