SELECT
    toDate(s.created_at) AS Date,
    s.id AS Study_ID,
    s.client_fk AS Client_ID,
    c.client_name AS Client_Name,
    s.order_id AS "5C_Order_ID",
    s.patient_id AS MR_No_,
    s.patient_age AS Patient_Age,
    arrayStringConcat(tokens(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\', '')), 'list')), ' ') AS Study_Name,
    s.modalities AS Modality,
    CASE
        WHEN length(JSONExtractArrayRaw(assumeNotNull(REPLACE(s.rules, '\\', '')), 'list')) >= 2 THEN 'NON-BIONIC'
        WHEN (
            arrayStringConcat(tokens(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\', '')), 'list')), ' ') IN (
                'CT Brain', 'CT PNS', 'XRAY Radiograph Chest', 'XRAY Radiograph Knee',
                'XRAY Radiograph Ankle', 'XRAY Radiograph Elbow', 'XRAY Radiograph Shoulder',
                'XRAY Radiograph Heel', 'XRAY Radiograph Foot', 'XRAY Radiograph Leg'
            )
            OR arrayStringConcat(tokens(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\', '')), 'list')), ' ') LIKE '%Spine%'
            OR arrayStringConcat(tokens(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\', '')), 'list')), ' ') ILIKE '%XRAY Radiograph Spine%'
        ) THEN 'BIONIC'
        ELSE 'NON-BIONIC'
    END AS Report_Type,
    ss.min_time AS Activated_Time_on_5C_Platform,
    ss2.status AS Status,
    CASE
        WHEN toHour(ss.min_time) >= 8 AND toHour(ss.min_time) < 20 THEN '8AM - 8PM'
        ELSE '8PM - 8AM'
    END AS Timeframe,
    toHour(ss.min_time) AS Hours,
    concat('https://admin.5cnetwork.com/cases/', s.id) AS Study_Link,
    formatDateTime(ss.min_time, '%Y-%m-%d %H:00:00') AS Date_Hour
FROM transform.Studies AS s
LEFT JOIN transform.Clients AS c ON s.client_fk = c.id
LEFT JOIN (
    SELECT study_fk, MIN(created_at) AS min_time
    FROM transform.StudyStatuses
    GROUP BY study_fk
) AS ss ON ss.study_fk = s.id
LEFT JOIN transform.StudyStatuses AS ss2 ON ss2.study_fk = s.id
WHERE lower(c.client_name) LIKE '%medanta%'
  AND ss2.status = 'PENDING'
  AND toDate(s.created_at) BETWEEN date_trunc('month', now()) AND toDate(now())
ORDER BY
    Client_Name,
    Study_ID
UNION ALL
SELECT
    NULL AS Date,
    NULL AS Study_ID,
    NULL AS Client_ID,
    NULL AS Client_Name,
    NULL AS "5C_Order_ID",
    NULL AS MR_No_,
    NULL AS Patient_Age,
    NULL AS Study_Name,
    NULL AS Modality,
    NULL AS Report_Type,
    NULL AS Activated_Time_on_5C_Platform,
    'PENDING' AS Status,
    NULL AS Timeframe,
    NULL AS Hours,
    NULL AS Study_Link,
    NULL AS Date_Hour
LIMIT 0;