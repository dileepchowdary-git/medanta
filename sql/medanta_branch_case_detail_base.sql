WITH latest_report_info AS (
    -- This CTE correctly identifies the latest report for each study
    SELECT
        study_fk,
        argMax(id, created_at) AS latest_report_id,
        argMax(rad_fk, created_at) AS latest_rad_fk,
        argMax(created_at, created_at) AS latest_report_time
    FROM transform.Reports
    GROUP BY study_fk
),
base AS (
    SELECT
        toDate(s.created_at) AS Date,
        s.id AS Study_ID,
        s.order_id AS "5C_Order_ID",
        s.patient_id AS MR_No_,
        s.client_fk AS Client_ID,
        c.client_name AS Client_Name,
        s.patient_age AS Patient_Age,
        length(JSONExtractArrayRaw(assumeNotNull(REPLACE(s.rules, '\\', '')), 'list')) AS Study_Count,
        arrayStringConcat(tokens(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\', '')), 'list')), ' ') AS Study_Name,
        s.modalities AS Modality,
        m.created_datetime AS Activated_Time_on_5C_Platform,
        m.completed_datetime AS Completed_Time_on_5C_Platform,
        m.tat_min AS Reported_TAT,
        TRUE AS TAT_Applicable,
        toHour(m.created_datetime) AS Hours,
        formatDateTime(m.created_datetime, '%Y-%m-%d %H:00:00') AS Date_Hour,
        concat('https://admin.5cnetwork.com/cases/', s.id) AS Study_Link,
        lri.latest_report_id AS latest_report_id
    FROM transform.Studies AS s
    LEFT JOIN transform.Clients AS c ON s.client_fk = c.id
    LEFT JOIN metrics.client_tat_metrics AS m ON m.study_id = s.id
    LEFT JOIN latest_report_info AS lri ON lri.study_fk = s.id
    WHERE lower(c.client_name) LIKE '%medanta%'
      AND toDate(s.created_at) BETWEEN date_trunc('month', now()) AND toDate(now())
)
SELECT
    Date,
    "5C_Order_ID",
    MR_No_,
    Study_ID,
    Client_ID,
    Client_Name,
    Patient_Age,
    Study_Name,
    Modality,
    CASE
        WHEN Study_Count >= 2 THEN 'NON-BIONIC'
        WHEN (
            Study_Name IN (
                'CT Brain', 'CT PNS', 'XRAY Radiograph Chest', 'XRAY Radiograph Knee',
                'XRAY Radiograph Ankle', 'XRAY Radiograph Elbow', 'XRAY Radiograph Shoulder',
                'XRAY Radiograph Heel', 'XRAY Radiograph Foot', 'XRAY Radiograph Leg'
            )
            OR Study_Name LIKE '%Spine%'
            OR Study_Name ILIKE '%XRAY Radiograph Spine%'
        ) THEN 'BIONIC'
        ELSE 'NON-BIONIC'
    END AS Report_Type,
    CASE
        WHEN last_reported.reported_as = 'BIONIC' THEN 'BIONIC'
        ELSE 'NON-BIONIC'
    END AS Reported_as,
    Hours,
    Activated_Time_on_5C_Platform,
    Completed_Time_on_5C_Platform,
    Reported_TAT,
    CASE
        WHEN latest_arm.findings IS NULL OR JSONExtractString(latest_arm.findings, 'findings', 'case_type', 'classification') = '' THEN 'No Classification Found'
        ELSE JSONExtractString(latest_arm.findings, 'findings', 'case_type', 'classification')
    END AS Classification,
    TAT_Applicable,
    Date_Hour,
    CASE
        WHEN Modality LIKE '%XRAY%' AND Reported_TAT > 60 THEN 1
        WHEN Modality LIKE '%CT%' AND Reported_TAT > 120 THEN 1
        WHEN Modality LIKE '%MRI%' AND Reported_TAT > 180 THEN 1
        WHEN Modality LIKE '%NM%' AND Reported_TAT > (24 * 60) THEN 1
        ELSE 0
    END AS Reported_TAT_breach,
    CASE
        WHEN Hours >= 8 AND Hours < 20 THEN '8AM - 8PM'
        ELSE '8PM - 8AM'
    END AS Timeframe,
    Study_Link
FROM base
-- This CTE correctly identifies the final reporting type based on the latest status change.
LEFT JOIN (
    SELECT
        study_fk,
        CASE
            WHEN argMax(by_user_fk, created_at) IN (1506, 1505, 2318, 1504, 2484, 2715, 2785) THEN 'BIONIC'
            ELSE 'NON-BIONIC'
        END AS reported_as
    FROM transform.StudyStatuses
    WHERE status = 'REPORTED'
    GROUP BY study_fk
) AS last_reported ON last_reported.study_fk = base.Study_ID
-- Join to AIModelResponses based on the LATEST report ID found in the base CTE.
LEFT JOIN (
    SELECT
        report_fk,
        argMax(findings, created_at) AS findings
    FROM transform.AIModelResponses
    GROUP BY report_fk
) AS latest_arm ON latest_arm.report_fk = base.latest_report_id
ORDER BY
    Client_Name,
    Study_ID;