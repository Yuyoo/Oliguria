DROP MATERIALIZED VIEW IF EXISTS oliguria_cohort_labevents CASCADE;
CREATE MATERIALIZED VIEW oliguria_cohort_labevents AS
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
SELECT base.subject_id,base.hadm_id,base.icustay_id,base.og_label,base.starttime,base.og_starttime,le.charttime,
    max(CASE
        WHEN itemid = 50821 	
          THEN valuenum
        ELSE NULL END) AS pO2,
    max(CASE
        WHEN itemid = 50818 
          THEN valuenum
        ELSE NULL END) AS pCO2,
    max(CASE
        WHEN itemid = 50820 
          THEN valuenum
        ELSE NULL END) AS arterial_ph,
    max(CASE
        WHEN itemid =50813
          THEN valuenum
        ELSE NULL END) AS Lactate,
    max(CASE
        WHEN itemid = 50802
          THEN valuenum
        ELSE NULL END) AS BE,
    max(CASE
        WHEN itemid = 50803
          THEN valuenum
        ELSE NULL END) AS Bicarbonate,
		max(CASE
        WHEN itemid = 50817 	
          THEN valuenum
        ELSE NULL END) AS SaO2,
    max(CASE
        WHEN itemid = 50804 
          THEN valuenum
        ELSE NULL END) AS Total_CO2,
    max(CASE
        WHEN itemid = 51222 
          THEN valuenum
        ELSE NULL END) AS Hemoglobin,
    max(CASE
        WHEN itemid =51279
          THEN valuenum
        ELSE NULL END) AS serum_rbc,
    max(CASE
        WHEN itemid = 51301
          THEN valuenum
        ELSE NULL END) AS serum_wbc,
    max(CASE
        WHEN itemid = 51265
          THEN valuenum
        ELSE NULL END) AS PLT,
    max(CASE
        WHEN itemid = 51256 	
          THEN valuenum
        ELSE NULL END) AS Neutrophils,
    max(CASE
        WHEN itemid = 51221 
          THEN valuenum
        ELSE NULL END) AS Hematocrit,
    max(CASE
        WHEN itemid = 51491 
          THEN valuenum
        ELSE NULL END) AS urine_ph,
    max(CASE
        WHEN itemid =51498
          THEN valuenum
        ELSE NULL END) AS urine_Specific_Gravity,
    max(CASE
        WHEN itemid = 51493
          THEN valuenum
        ELSE NULL END) AS urine_rbc,
    max(CASE
        WHEN itemid = 51516
          THEN valuenum
        ELSE NULL END) AS urine_wbc,
		max(CASE
        WHEN itemid = 51476 	
          THEN valuenum
        ELSE NULL END) AS Epithelial_Cells,
    max(CASE
        WHEN itemid = 51492 
          THEN valuenum
        ELSE NULL END) AS urine_Protein,
    max(CASE
        WHEN itemid = 51487 
          THEN valuenum
        ELSE NULL END) AS Nitrite,
    max(CASE
        WHEN itemid =50861
          THEN valuenum
        ELSE NULL END) AS ALT,
    max(CASE
        WHEN itemid = 50878
          THEN valuenum
        ELSE NULL END) AS AST,
    max(CASE
        WHEN itemid = 50862
          THEN valuenum
        ELSE NULL END) AS serum_Albumin,
    max(CASE
        WHEN itemid = 50976 	
          THEN valuenum
        ELSE NULL END) AS Total_Protein,
    max(CASE
        WHEN itemid = 50883 
          THEN valuenum
        ELSE NULL END) AS Bilirubin_Direct,
    max(CASE
        WHEN itemid = 50884 
          THEN valuenum
        ELSE NULL END) AS Bilirubin_Indirect,
    max(CASE
        WHEN itemid =51003
          THEN valuenum
        ELSE NULL END) AS Troponin_T,
    max(CASE
        WHEN itemid = 50863
          THEN valuenum
        ELSE NULL END) AS ALP,
    max(CASE
        WHEN itemid = 50927
          THEN valuenum
        ELSE NULL END) AS r_GT,
		max(CASE
        WHEN itemid = 50931 	
          THEN valuenum
        ELSE NULL END) AS Glucose,
    max(CASE
        WHEN itemid = 51006 
          THEN valuenum
        ELSE NULL END) AS BUN,
    max(CASE
        WHEN itemid = 50912 
          THEN valuenum
        ELSE NULL END) AS Creatinine,
    max(CASE
        WHEN itemid =51007
          THEN valuenum
        ELSE NULL END) AS Uric_Acid,
    max(CASE
        WHEN itemid = 50910
          THEN valuenum
        ELSE NULL END) AS Creatine_Kinase,
    max(CASE
        WHEN itemid = 50889
          THEN valuenum
        ELSE NULL END) AS CRP,
    max(CASE
        WHEN itemid = 50954 	
          THEN valuenum
        ELSE NULL END) AS LD,
    max(CASE
        WHEN itemid = 50893 
          THEN valuenum
        ELSE NULL END) AS Calcium,
    max(CASE
        WHEN itemid = 50970 
          THEN valuenum
        ELSE NULL END) AS Phosphate,
    max(CASE
        WHEN itemid =50960
          THEN valuenum
        ELSE NULL END) AS Magnesium,
    max(CASE
        WHEN itemid = 50971
          THEN valuenum
        ELSE NULL END) AS Potassium,
    max(CASE
        WHEN itemid = 50983
          THEN valuenum
        ELSE NULL END) AS Sodium,
		max(CASE
        WHEN itemid = 50902 	
          THEN valuenum
        ELSE NULL END) AS Chloride,
    max(CASE
        WHEN itemid = 50867 
          THEN valuenum
        ELSE NULL END) AS Amylase,
    max(CASE
        WHEN itemid = 50956 
          THEN valuenum
        ELSE NULL END) AS Lipase,
    max(CASE
        WHEN itemid =50911
          THEN valuenum
        ELSE NULL END) AS CK_MB,
    max(CASE
        WHEN itemid = 50963
          THEN valuenum
        ELSE NULL END) AS NTproBNP,
    max(CASE
        WHEN itemid = 51297
          THEN valuenum
        ELSE NULL END) AS Thrombin,	
    max(CASE
        WHEN itemid = 51275 
          THEN valuenum
        ELSE NULL END) AS PTT,
    max(CASE
        WHEN itemid =51274
          THEN valuenum
        ELSE NULL END) AS PT,
    max(CASE
        WHEN itemid = 51237
          THEN valuenum
        ELSE NULL END) AS INR,
    max(CASE
        WHEN itemid = 51214
          THEN valuenum
        ELSE NULL END) AS Fibrinogen,		
		max(CASE
        WHEN itemid = 51196
          THEN valuenum
        ELSE NULL END) AS D_Dimer
FROM base
left join labevents le
on base.hadm_id = le.hadm_id
and le.charttime BETWEEN (base.starttime - INTERVAL '6' hour) and (base.starttime + INTERVAL '24' hour)
WHERE le.itemid in (
-- blood gas
 50821	-- pO2
, 50818	-- pCO2
, 50820	-- pH
, 50813 -- Lactate
, 50802 -- Base Excess
, 50803	-- Calculated Bicarbonate, Whole Blood
, 50817	-- Oxygen Saturation
, 50804	-- Calculated Total CO2
-- blood culture
, 51222	-- Hemoglobin
, 51279	-- Red Blood Cells
, 51301	-- White Blood Cells
, 51265	-- Platelet Count
, 51256	-- Neutrophils
, 51221	-- Hematocrit
-- urine culture
, 51491 -- pH | 尿酸碱度
, 51498	-- Specific Gravity | 尿比重
, 51493	-- RBC | 尿红细胞
, 51516	-- WBC | 尿白细胞
, 51476	-- Epithelial Cells	| 上皮细胞
, 51492	-- Protein | 尿蛋白
, 51487	-- Nitrite | 尿亚硝酸盐
-- blood chemistry
, 50861	-- Alanine Aminotransferase (ALT) | 丙氨酸氨基转移酶
, 50878	-- Asparate Aminotransferase (AST) | 天冬氨酸氨基转移酶
, 50862	-- Albumin | 血清蛋白
, 50976 -- Protein, Total | 总蛋白
, 50883	-- Bilirubin, Direct | 直接胆红素
, 50884	-- Bilirubin, Indirect | 间接胆红素
, 51003	-- Troponin T | 肌钙蛋白T
, 50863 -- Alkaline Phosphatase | 碱性磷酸酶
, 50927	-- Gamma Glutamyltransferase | r-谷氨酰基转移酶
, 50931	-- Glucose | 葡萄糖
, 51006 -- Urea Nitrogen | 尿素氮
, 50912 -- Creatinine | 肌酐
, 51007	-- Uric Acid | 尿酸
, 50910 -- Creatine Kinase (CK) | 肌酸激酶
, 50889 -- C-Reactive Protein | C反应蛋白
, 50954 -- Lactate Dehydrogenase (LD) | 乳酸脱氢酶
, 50893 -- Calcium, Total	| 钙
, 50970 -- Phosphate | 磷
, 50960 -- Magnesium | 镁
, 50971 -- Potassium | 钾
, 50983 -- Sodium | 钠
, 50902 -- Chloride | 氯化物
, 50867 -- Amylase | 淀粉酶
, 50956 -- Lipase | 脂肪酶
, 50911 -- Creatine Kinase, MB Isoenzyme | 肌酸激酶同工酶定量
, 50963 -- NTproBNP | 脑利钠肽前体
-- Coagulation | 凝血
, 51297 -- Thrombin | 凝血酶时间测定
, 51275 -- PTT | 血浆活化部分凝血活酶时间测定
, 51274 -- PT | 血浆凝血酶原时间测定
, 51237 -- INR | 国际标准化比值
, 51214 -- Fibrinogen | 血浆纤维蛋白原测定
, 51196 -- D-Dimer | 血浆D二聚体测定
)
AND valuenum IS NOT null AND valuenum > 0 -- lab values cannot be 0 and cannot be negative
GROUP BY base.subject_id,base.hadm_id,base.icustay_id,base.og_label,base.starttime,base.og_starttime,le.charttime 