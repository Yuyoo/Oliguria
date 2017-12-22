DROP MATERIALIZED VIEW IF EXISTS oliguria_labevents;
CREATE MATERIALIZED VIEW oliguria_labevents AS
  SELECT
    og.subject_id,
    og.hadm_id,
    og.icustay_id,
    le.charttime,
    --动脉氧分压
    max(CASE
        WHEN itemid = 50821 AND valuenum > 0 AND valuenum < 800
          THEN valuenum
        ELSE NULL END) AS PO2,
    --动脉二氧化碳分压
    max(CASE
        WHEN itemid = 50818 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS PCO2,
    --动脉血ph
    max(CASE
        WHEN itemid = 50820 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS ArterialpH,
    --血钾
    max(CASE
        WHEN itemid = 50822 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Potassium,
    --血钙
    max(CASE
        WHEN itemid = 50808 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Calcium,
    --血钠
    max(CASE
        WHEN itemid = 50824 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Sodium,
    --血氯
    max(CASE
        WHEN itemid = 50806 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Chloride,
    --血糖
    max(CASE
        WHEN itemid = 50809 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Glucose,
    --重碳酸盐HCO3-
    max(CASE
        WHEN itemid = 50882 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Bicarbonate,
    --碱剩余
    max(CASE
        WHEN itemid = 50802 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS BE,
    --动脉血白细胞
    max(CASE
        WHEN itemid = 51301 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Arterialwbc,
    --动脉血红细胞
    max(CASE
        WHEN itemid = 51279 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Arterialrbc,
    --血小板计数
    max(CASE
        WHEN itemid = 51265 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS PLT,
    --C反应蛋白
    max(CASE
        WHEN itemid = 50889 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS CRP,
    --血乳酸
    max(CASE
        WHEN itemid = 50813 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Lactate,
    --谷丙转氨酶
    max(CASE
        WHEN itemid = 50861 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS ALT,
    --谷草转氨酶
    max(CASE
        WHEN itemid = 50878 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS AST,
    --血肌酐
    max(CASE
        WHEN itemid = 50912 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Creatinine,
    --血尿素氮
    max(CASE
        WHEN itemid = 51006 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS BUN,
    --淀粉酶
    max(CASE
        WHEN itemid = 50867 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Amylase,
    --脂肪酶
    max(CASE
        WHEN itemid = 50956 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Lipase,
    --尿酸碱度
    max(CASE
        WHEN itemid = 51094 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Urine_pH,
    --尿白细胞
    max(CASE
        WHEN itemid = 51516 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Urine_wbc,
    --尿蛋白
    max(CASE
        WHEN itemid = 51492 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Urine_Protein,
    --尿糖
    max(CASE
        WHEN itemid = 51478 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Urine_Glucose,
    --尿胆红素
    max(CASE
        WHEN itemid = 51464 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Urine_Bilirubin,
    --酮体
    max(CASE
        WHEN itemid = 51484 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Urine_Ketone,
    --尿红细胞
    max(CASE
        WHEN itemid = 51493 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Urine_RBC,
    --尿比重
    max(CASE
        WHEN itemid = 51498 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS SpecificGravity,
    --尿胆原
    max(CASE
        WHEN itemid = 51514 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS Urobilinogen,
    --尿隐血
    max(CASE
        WHEN itemid = 51466
          THEN le.value
        ELSE NULL END) AS Urine_Blood,
    --尿液颜色
    max(CASE
        WHEN itemid = 51508
          THEN le.value
        ELSE NULL END) AS Urine_Color,
    --活化部分凝血活酶时间
    max(CASE
        WHEN itemid = 51275 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS APTT,
    --凝血酶原时间
    max(CASE
        WHEN itemid = 51274 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS PT,
    --国际标准化比值
    max(CASE
        WHEN itemid = 51237 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS INR,
    --D-二聚体
    max(CASE
        WHEN itemid = 51196 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS D_Dimer,
    --纤维蛋白原
    max(CASE
        WHEN itemid = 51214 AND valuenum > 0
          THEN valuenum
        ELSE NULL END) AS FIB
  FROM public.oliguria og
    LEFT JOIN labevents le
      ON og.subject_id = le.subject_id
         AND og.hadm_id = le.hadm_id
         AND le.charttime BETWEEN (og.intime - INTERVAL '6' HOUR) AND og.uo_charttime2
  WHERE le.itemid IN (
    50821, --PO2 动脉氧分压
           50818, --PCO2  动脉二氧化碳分压
           50820, --pH  动脉血pH
           50822, --Potassium 血钾（血气）
           50808, --Free Calcium 血钙（血气）
           50824, --Sodium, Whole Blood 血钠（血气）
           50806, --Chloride, Whole Blood 血氯（血气）
           50809, --Glucose 血糖（血气）
           50882, --Bicarbonate 重碳酸HCO3-
           50802, --Base Excess 碱剩余
           51301, --wbc 白细胞计数
                  51279, --rbc 红细胞计数
                  51265, --Platelet Count  血小板计数
                  50889, --C-Reactive Protein  C反应蛋白
                  50813, --Lactate 血乳酸（血气）
                  50861, --ALT
                  50878, --AST
                  50912, --Creatinine  血肌酐
                  51006, --Urea Nitrogen 尿素氮
                  50867, --Amylase 淀粉酶
                  50956, --Lipase  脂肪酶
                         51094, --pH   尿pH
                         51516, --wbc 尿wbc
                         51492, --Protein 尿蛋白
                         51478, --Glucose 尿糖
                         51464, --Bilirubin 尿胆红素
                         51484, --Ketone  酮体
                         51493, --RBC 尿红细胞
                         51498, --Specific Gravity  尿比重
                         51514, --Urobilinogen  尿胆原
                         51466, --Blood 尿隐血
    51508, --Urine Color 尿液颜色
    51275, --PTT  活化部分凝血活酶时间
    51274, --PT  凝血酶原时间
    51237, --INR 国际标准化比值
    51196, --D-Dimer D-二聚体
    51214 --Fibrinogen  纤维蛋白原
  )
  GROUP BY og.subject_id, og.hadm_id, og.icustay_id, le.charttime


