-- This query extracts weights for adult ICU patients with start/stop times
-- if an admission weight is given, then this is assigned from intime to outtime

DROP MATERIALIZED VIEW IF EXISTS weightdurations CASCADE;
CREATE MATERIALIZED VIEW weightdurations AS

  -- This query extracts weights for adult ICU patients with start/stop times
  -- if an admission weight is given, then this is assigned from intime to outtime
  WITH wt_stg AS
  (
      SELECT
        c.icustay_id,
        c.charttime,
        CASE WHEN c.itemid IN (762, 226512)
          THEN 'admit'
        ELSE 'daily' END AS weight_type
        -- TODO: eliminate obvious outliers if there is a reasonable weight
        ,
        c.valuenum       AS weight
      FROM chartevents c
      WHERE c.valuenum IS NOT NULL
            AND c.itemid IN
                (
                  762, 226512 -- Admit Wt
                  , 763, 224639 -- Daily Weight
                )
            AND c.valuenum != 0
            -- exclude rows marked as error
            AND c.error IS DISTINCT FROM 1
  )
    -- assign ascending row number
    , wt_stg1 AS
  (
      SELECT
        icustay_id,
        charttime,
        weight_type,
        weight,
        ROW_NUMBER()
        OVER (
          PARTITION BY icustay_id, weight_type
          ORDER BY charttime ) AS rn
      FROM wt_stg
  )
    -- change charttime to starttime - for admit weight, we use ICU admission time
    , wt_stg2 AS
  (
      SELECT
        wt_stg1.icustay_id,
        ie.intime,
        ie.outtime,
        CASE WHEN wt_stg1.weight_type = 'admit' AND wt_stg1.rn = 1
          THEN ie.intime - INTERVAL '2' HOUR
        ELSE wt_stg1.charttime END AS starttime,
        wt_stg1.weight
      FROM icustays ie
        INNER JOIN wt_stg1
          ON ie.icustay_id = wt_stg1.icustay_id
      WHERE NOT (weight_type = 'admit' AND rn = 1)
  )
    , wt_stg3 AS
  (
      SELECT
        icustay_id,
        starttime,
        coalesce(
            LEAD(starttime)
            OVER (
              PARTITION BY icustay_id
              ORDER BY starttime ),
            outtime + INTERVAL '2' HOUR
        ) AS endtime,
        weight
      FROM wt_stg2
  )
    -- this table is the start/stop times from admit/daily weight in charted data
    , wt1 AS
  (
      SELECT
        ie.icustay_id,
        wt.starttime,
        CASE WHEN wt.icustay_id IS NULL
          THEN NULL
        ELSE
          coalesce(wt.endtime,
                   LEAD(wt.starttime)
                   OVER (
                     PARTITION BY ie.icustay_id
                     ORDER BY wt.starttime ),
                   -- we add a 2 hour "fuzziness" window
                   ie.outtime + INTERVAL '2' HOUR)
        END AS endtime,
        wt.weight
      FROM icustays ie
        LEFT JOIN wt_stg3 wt
          ON ie.icustay_id = wt.icustay_id
  )
    -- if the intime for the patient is < the first charted daily weight
    -- then we will have a "gap" at the start of their stay
    -- to prevent this, we look for these gaps and backfill the first weight
    -- this adds (153255-149657)=3598 rows, meaning this fix helps for up to 3598 icustay_id
    , wt_fix AS
  (
      SELECT
        ie.icustay_id
        -- we add a 2 hour "fuzziness" window
        ,
        ie.intime - INTERVAL '2' HOUR AS starttime,
        wt.starttime                  AS endtime,
        wt.weight
      FROM icustays ie
        INNER JOIN
        -- the below subquery returns one row for each unique icustay_id
        -- the row contains: the first starttime and the corresponding weight
        (
          SELECT
            wt1.icustay_id,
            wt1.starttime,
            wt1.weight
          FROM wt1
            INNER JOIN
            (
              SELECT
                icustay_id,
                min(Starttime) AS starttime
              FROM wt1
              GROUP BY icustay_id
            ) wt2
              ON wt1.icustay_id = wt2.icustay_id
                 AND wt1.starttime = wt2.starttime
        ) wt
          ON ie.icustay_id = wt.icustay_id
             AND ie.intime < wt.starttime
  )
    , wt2 AS
  (
    SELECT
      wt1.icustay_id,
      wt1.starttime,
      wt1.endtime,
      wt1.weight
    FROM wt1
    UNION
    SELECT
      wt_fix.icustay_id,
      wt_fix.starttime,
      wt_fix.endtime,
      wt_fix.weight
    FROM wt_fix
  )
    -- get more weights from echo - completes data for ~2500 patients
    -- we only use echo data if there is *no* charted data
    -- we impute the median echo weight for their entire ICU stay
    -- only ~762 patients remain with no weight data
    , echo_lag AS
  (
      SELECT
        ie.icustay_id,
        ie.intime,
        ie.outtime,
        0.453592 * ec.weight      AS weight_echo,
        ROW_NUMBER()
        OVER (
          PARTITION BY ie.icustay_id
          ORDER BY ec.charttime ) AS rn,
        ec.charttime              AS starttime,
        LEAD(ec.charttime)
        OVER (
          PARTITION BY ie.icustay_id
          ORDER BY ec.charttime ) AS endtime
      FROM icustays ie
        INNER JOIN echodata ec
          ON ie.hadm_id = ec.hadm_id
      WHERE ec.weight IS NOT NULL
  )
    , echo_final AS
  (
    SELECT
      el.icustay_id,
      el.starttime
      -- we add a 2 hour "fuzziness" window
      ,
      coalesce(el.endtime, el.outtime + INTERVAL '2' HOUR) AS endtime,
      weight_echo
    FROM echo_lag el
    UNION
    -- if the starttime was later than ICU admission, back-propogate the weight
    SELECT
      el.icustay_id,
      el.intime - INTERVAL '2' HOUR AS starttime,
      el.starttime                  AS endtime,
      el.weight_echo
    FROM echo_lag el
    WHERE el.rn = 1
          AND el.starttime > el.intime - INTERVAL '2' HOUR
  )
  SELECT
    wt2.icustay_id,
    wt2.starttime,
    wt2.endtime,
    wt2.weight
  FROM wt2
  UNION
  -- only add echos if we have no charted weight data
  SELECT
    ef.icustay_id,
    ef.starttime,
    ef.endtime,
    ef.weight_echo AS weight
  FROM echo_final ef
  WHERE ef.icustay_id NOT IN (SELECT DISTINCT icustay_id
                              FROM wt2)
  ORDER BY icustay_id, starttime, endtime;