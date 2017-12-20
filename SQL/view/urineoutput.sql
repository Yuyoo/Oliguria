-- First we drop the table if it exists
DROP MATERIALIZED VIEW IF EXISTS urineoutput CASCADE;
CREATE MATERIALIZED VIEW urineoutput AS
  SELECT
    oe.icustay_id,
    oe.charttime,
    SUM(
    -- we consider input of GU irrigant as a negative volume
        CASE WHEN oe.itemid = 227488
          THEN -1 * value
        ELSE value END
    ) AS value
  FROM outputevents oe
  WHERE oe.itemid IN
        (
          -- these are the most frequently occurring urine output observations in CareVue
          40055, -- "Urine Out Foley"
                 43175, -- "Urine ."
                 40069, -- "Urine Out Void"
                 40094, -- "Urine Out Condom Cath"
                 40715, -- "Urine Out Suprapubic"
                 40473, -- "Urine Out IleoConduit"
                 40085, -- "Urine Out Incontinent"
                 40057, -- "Urine Out Rt Nephrostomy"
                 40056, -- "Urine Out Lt Nephrostomy"
                 40405, -- "Urine Out Other"
                 40428, -- "Urine Out Straight Cath"
                        40086, --	Urine Out Incontinent
                        40096, -- "Urine Out Ureteral Stent #1"
                        40651, -- "Urine Out Ureteral Stent #2"

                        -- these are the most frequently occurring urine output observations in MetaVision
                        226559, -- "Foley"
                        226560, -- "Void"
                        226561, -- "Condom Cath"
                        226584, -- "Ileoconduit"
                        226563, -- "Suprapubic"
                        226564, -- "R Nephrostomy"
                        226565, -- "L Nephrostomy"
          226567, --	Straight Cath
          226557, -- R Ureteral Stent
          226558, -- L Ureteral Stent
          227488, -- GU Irrigant Volume In
          227489  -- GU Irrigant/Urine Volume Out
        )
        AND oe.value < 5000 -- sanity check on urine value
        AND oe.icustay_id IS NOT NULL
  GROUP BY icustay_id, charttime;