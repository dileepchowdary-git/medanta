WITH latest_report AS (
    -- Find the single, latest report for each study
    SELECT
        study_fk,
        argMax(id, created_at) AS latest_report_id,
        argMax(created_at, created_at) AS last_report_time,
        argMax(rad_fk, created_at) AS latest_rad_fk
    FROM Reports
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
        arrayStringConcat(tokens(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\', '')), 'list')), ' ') AS Study_Name,
        length(JSONExtractArrayRaw(assumeNotNull(REPLACE(s.rules, '\\', '')), 'list')) AS Study_Count,
        s.patient_age AS Patient_Age,
        CASE
            WHEN s.modalities = 'XRAY' AND simpleJSONExtractInt(assumeNotNull(REPLACE(s.rules, '\\', '')), 'mod_study') = 43 THEN 'XRAY Special'
            ELSE s.modalities
        END AS Modality,
        ss.min_time AS Activated_Time_on_5C_Platform,
        sc.max_time AS Completed_Time_on_5C_Platform,
        toHour(ss.min_time) AS Hours,
        m.tat_min AS TAT_minutes_,
        concat('https://admin.5cnetwork.com/cases/', s.id) AS Study_Link,
        formatDateTime(ss.min_time, '%Y-%m-%d %H:00:00') AS Date_Hour,
        lr.latest_report_id AS Report_ID,
        lr.latest_rad_fk AS Rad_FK,
        latest_arm.findings AS Latest_Findings
    FROM Studies AS s
    LEFT JOIN Clients AS c ON s.client_fk = c.id
    LEFT JOIN (
        SELECT study_fk, MIN(created_at) AS min_time
        FROM StudyStatuses
        WHERE status = 'ASSIGNED'
        GROUP BY study_fk
    ) AS ss ON ss.study_fk = s.id
    LEFT JOIN (
        SELECT study_fk, MAX(created_at) AS max_time
        FROM StudyStatuses
        WHERE status = 'COMPLETED'
        GROUP BY study_fk
    ) AS sc ON sc.study_fk = s.id
    LEFT JOIN metrics.client_tat_metrics AS m ON m.study_id = s.id
    LEFT JOIN latest_report AS lr ON lr.study_fk = s.id
    LEFT JOIN (
        SELECT
            report_fk,
            argMax(findings, created_at) AS findings
        FROM AIModelResponses
        GROUP BY report_fk
    ) AS latest_arm ON latest_arm.report_fk = lr.latest_report_id
    WHERE lower(c.client_name) LIKE '%medanta%'
      AND toDate(s.created_at) BETWEEN date_trunc('month', now()) AND toDate(now())
),
rejection_info AS (
    SELECT
        r.study_fk AS study_fk,
        COUNTIf(rq.status IN ('REJECTED', 'EQC_REJECTED')) AS Rejected_Times,
        argMax(rq.created_at, IF(rq.status IN ('REJECTED', 'EQC_REJECTED'), rq.created_at, NULL)) AS Rejection_Time,
        argMax(concat(a.first_name, ' ', a.last_name), IF(rq.status IN ('REJECTED', 'EQC_REJECTED'), rq.created_at, NULL)) AS Rejects_Rad_Name,
        MAX(CASE WHEN rq.status = 'EQC_REJECTED' THEN 1 ELSE 0 END) > 0 AS eqc_rejects,
        COUNTIf(rq.status = 'EQC_REJECTED') AS Total_EQC_Rejected_Times,
        MAX(rq.name) AS QC_Name
    FROM ReportQcs AS rq
    LEFT JOIN Reports AS r ON rq.report_fk = r.id
    LEFT JOIN Ancillaries AS a ON r.rad_fk = a.id
    GROUP BY r.study_fk
),
last_rad_info AS (
    -- The last completed rad is already available in the 'base' CTE's 'Rad_FK'
    -- We'll just join with Ancillaries to get the name
    SELECT
        a.id AS rad_id,
        concat(a.first_name, ' ', a.last_name) AS Last_Completed_Rad
    FROM Ancillaries AS a
)
SELECT
    b.Date,
    b."5C_Order_ID",
    b.Study_ID,
    b.MR_No_,
    b.Client_ID,
    b.Client_Name,
    b.Study_Name,
    b.Patient_Age,
    b.Modality,
    CASE
        WHEN b.Study_Count >= 2 THEN 'NON-BIONIC'
        WHEN (
            b.Study_Name IN (
                'CT Brain', 'CT PNS', 'XRAY Radiograph Chest', 'XRAY Radiograph Knee',
                'XRAY Radiograph Ankle', 'XRAY Radiograph Elbow', 'XRAY Radiograph Shoulder',
                'XRAY Radiograph Heel', 'XRAY Radiograph Foot', 'XRAY Radiograph Leg'
            )
            OR b.Study_Name LIKE '%Spine%'
            OR b.Study_Name ILIKE '%XRAY Radiograph Spine%'
        ) THEN 'BIONIC'
        ELSE 'NON-BIONIC'
    END AS Report_Type,
    b.Hours,
    CASE
        WHEN b.Hours >= 8 AND b.Hours < 20 THEN '8AM - 8PM'
        ELSE '8PM - 8AM'
    END AS Timeframe,
    b.Activated_Time_on_5C_Platform,
    b.Completed_Time_on_5C_Platform,
    b.TAT_minutes_,
    CASE
        WHEN b.Latest_Findings IS NULL OR JSONExtractString(b.Latest_Findings, 'findings', 'case_type', 'classification') = '' THEN 'No Classification Found'
        ELSE JSONExtractString(b.Latest_Findings, 'findings', 'case_type', 'classification')
    END AS Classification,
    rj.Rejects_Rad_Name,
    rj.Rejected_Times AS Rejected_Reports,
    rj.Rejection_Time,
    rj.QC_Name,
    rj.eqc_rejects,
    lr_info.Last_Completed_Rad,
    rj.Total_EQC_Rejected_Times,
    b.Study_Link,
    rj.Rejected_Times AS reject_count,
    rj.Rejected_Times AS rejection_frequency,
    b.Completed_Time_on_5C_Platform AS Last_Reported_date,
    b.Date_Hour
FROM base AS b
LEFT JOIN rejection_info rj ON rj.study_fk = b.Study_ID
LEFT JOIN last_rad_info lr_info ON lr_info.rad_id = b.Rad_FK
ORDER BY
    Client_Name,
    Study_ID;