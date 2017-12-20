-- This code extracts structured data from echocardiographies
-- You can join it to the text notes using ROW_ID
-- Just note that ROW_ID will differ across versions of MIMIC-III.

DROP MATERIALIZED VIEW IF EXISTS ECHODATA CASCADE;
CREATE MATERIALIZED VIEW ECHODATA AS
  SELECT
    ROW_ID,
    subject_id,
    hadm_id,
    chartdate

    -- charttime is always null for echoes..
    -- however, the time is available in the echo text, e.g.:
    -- , substring(ne.text, 'Date/Time: [\[\]0-9*-]+ at ([0-9:]+)') as TIMESTAMP
    -- we can therefore impute it and re-create charttime
    ,
    cast(to_timestamp((to_char(chartdate, 'DD-MM-YYYY') || substring(ne.text, 'Date/Time: [\[\]0-9*-]+ at ([0-9:]+)')),
                      'DD-MM-YYYYHH24:MI') AS TIMESTAMP WITHOUT TIME ZONE)
                                                     AS charttime

    -- explanation of below substring:
    --  'Indication: ' - matched verbatim
    --  (.*?) - match any character
    --  \n - the end of the line
    -- substring only returns the item in ()s
    -- note: the '?' makes it non-greedy. if you exclude it, it matches until it reaches the *last* \n

    ,
    substring(ne.text, 'Indication: (.*?)\n')        AS Indication

    -- sometimes numeric values contain de-id text, e.g. [** Numeric Identifier **]
    -- this removes that text
    ,
    CASE
    WHEN substring(ne.text, 'Height: \(in\) (.*?)\n') LIKE '%*%'
      THEN NULL
    ELSE cast(substring(ne.text, 'Height: \(in\) (.*?)\n') AS NUMERIC)
    END                                              AS Height,
    CASE
    WHEN substring(ne.text, 'Weight \(lb\): (.*?)\n') LIKE '%*%'
      THEN NULL
    ELSE cast(substring(ne.text, 'Weight \(lb\): (.*?)\n') AS NUMERIC)
    END                                              AS Weight,
    CASE
    WHEN substring(ne.text, 'BSA \(m2\): (.*?) m2\n') LIKE '%*%'
      THEN NULL
    ELSE cast(substring(ne.text, 'BSA \(m2\): (.*?) m2\n') AS NUMERIC)
    END                                              AS BSA -- ends in 'm2'

    ,
    substring(ne.text, 'BP \(mm Hg\): (.*?)\n')      AS BP -- Sys/Dias

    ,
    CASE
    WHEN substring(ne.text, 'BP \(mm Hg\): ([0-9]+)/[0-9]+?\n') LIKE '%*%'
      THEN NULL
    ELSE cast(substring(ne.text, 'BP \(mm Hg\): ([0-9]+)/[0-9]+?\n') AS NUMERIC)
    END                                              AS BPSys -- first part of fraction

    ,
    CASE
    WHEN substring(ne.text, 'BP \(mm Hg\): [0-9]+/([0-9]+?)\n') LIKE '%*%'
      THEN NULL
    ELSE cast(substring(ne.text, 'BP \(mm Hg\): [0-9]+/([0-9]+?)\n') AS NUMERIC)
    END                                              AS BPDias -- second part of fraction

    ,
    CASE
    WHEN substring(ne.text, 'HR \(bpm\): ([0-9]+?)\n') LIKE '%*%'
      THEN NULL
    ELSE cast(substring(ne.text, 'HR \(bpm\): ([0-9]+?)\n') AS NUMERIC)
    END                                              AS HR,
    substring(ne.text, 'Status: (.*?)\n')            AS Status,
    substring(ne.text, 'Test: (.*?)\n')              AS Test,
    substring(ne.text, 'Doppler: (.*?)\n')           AS Doppler,
    substring(ne.text, 'Contrast: (.*?)\n')          AS Contrast,
    substring(ne.text, 'Technical Quality: (.*?)\n') AS TechnicalQuality
  FROM noteevents ne
  WHERE category = 'Echo';