WITH base AS (  
      SELECT  
          toDate(s.created_at) AS Date,  
          toHour(ss_assigned.min_time) AS Hours,  
          s.order_id AS "5C_Order_ID",  
          s.patient_id AS MR_No_,  
          s.id AS Study_ID,  
          s.client_fk AS Client_ID,  
          c.client_name AS Client_Name,  
          s.patient_age AS Patient_Age,  
          arrayStringConcat(tokens(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\', '')), 'list')), ' ') AS Study_Name,  
          CASE
              WHEN s.modalities = 'XRAY' AND simpleJSONExtractInt(assumeNotNull(REPLACE(s.rules, '\\', '')), 'mod_study') = 43 THEN 'XRAY Special'
              ELSE s.modalities
          END AS Modality,  
          ss_assigned.min_time AS Activated_Time_on_5C_Platform,  
          ss_completed.max_time AS Completed_Time_on_5C_Platform,  
          m.tat_min AS TAT_minutes_,  
          s.id AS study_id,  
          concat('https://admin.5cnetwork.com/cases/', s.id) AS Study_Link,  
          formatDateTime(ss_assigned.min_time, '%Y-%m-%d %H:00:00') AS Date_Hour,  
          lr.rad_fk AS rad_fk_latest,  
          asr.id AS Reason_ID,  
          asr.reason AS reason
      FROM transform.Studies AS s  
      LEFT JOIN transform.Clients AS c ON s.client_fk = c.id  
      LEFT JOIN (  
          SELECT study_fk, MIN(created_at) AS min_time  
          FROM transform.StudyStatuses  
          WHERE status = 'ASSIGNED'  
          GROUP BY study_fk  
      ) AS ss_assigned ON ss_assigned.study_fk = s.id  
      LEFT JOIN (  
          SELECT study_fk, MAX(created_at) AS max_time  
          FROM transform.StudyStatuses  
          WHERE status = 'COMPLETED'  
          GROUP BY study_fk  
      ) AS ss_completed ON ss_completed.study_fk = s.id  
      LEFT JOIN metrics.client_tat_metrics AS m ON m.study_id = s.id  
      LEFT JOIN (  
          SELECT  
              study_fk,  
              argMax(id, created_at) AS id,  
              argMax(rad_fk, created_at) AS rad_fk  
          FROM transform.Reports  
          GROUP BY study_fk  
      ) AS lr ON lr.study_fk = s.id  
      LEFT JOIN transform.ModStudies AS b ON b.id = s.id  
      LEFT JOIN (
          SELECT
              study_fk,
              argMax(reason_fk, created_at) AS latest_reason_fk
          FROM AIStudyLogs
          GROUP BY study_fk
      ) AS asl ON asl.study_fk = s.id
      LEFT JOIN AISkipReasons AS asr ON asr.id = asl.latest_reason_fk
      WHERE lower(c.client_name) LIKE '%medanta%'  
  )  
  SELECT  
      Date,  
      Hours,  
      "5C_Order_ID",  
      MR_No_,  
      Study_ID,  
      Client_ID,  
      Client_Name,  
      Patient_Age,  
      Study_Name,  
      Modality,  
      arrayElement(splitByChar(' ', Study_Name), -1) AS Organ,  
      (CASE  
          WHEN (  
              Study_Name LIKE '%CT Brain%'  
              OR Study_Name LIKE '%CT PNS%'  
              OR Study_Name LIKE '%Spine%'  
              OR Study_Name LIKE '%XRAY Radiograph Chest%'  
              OR Study_Name LIKE '%XRAY Radiograph Knee%'  
              OR Study_Name ILIKE '%XRAY Radiograph Spine%'  
              OR Study_Name LIKE '%XRAY Radiograph Ankle%'  
              OR Study_Name LIKE '%XRAY Radiograph Elbow%'  
              OR Study_Name LIKE '%XRAY Radiograph Shoulder%'  
              OR Study_Name LIKE '%XRAY Radiograph Heel%'  
              OR Study_Name LIKE '%XRAY Radiograph Foot%'  
              OR Study_Name LIKE '%XRAY Radiograph Leg%'  
              ) THEN 'BIONIC'  
          ELSE 'NON-BIONIC'  
      END) AS In_Report_Type,  
      CASE  
          WHEN rad_fk_latest IN (1506, 1505, 2318, 1504, 2484, 2715, 2785) THEN 'BIONIC'  
          ELSE 'NON-BIONIC'  
      END AS Out_Report_Type,  
      CASE  
          WHEN In_Report_Type = 'BIONIC' AND Out_Report_Type = 'BIONIC' THEN 'True'  
          ELSE 'False'  
      END AS Check,  
      Activated_Time_on_5C_Platform,  
      Completed_Time_on_5C_Platform,  
      TAT_minutes_,  
      study_id,  
      Reason_ID,  
      Study_Link,  
      reason,  
      Date_Hour  
  FROM base  
  WHERE Date BETWEEN date_trunc('month', today()) AND today()  
      AND In_Report_Type = 'BIONIC'  
  ORDER BY  
      Client_Name,  
      Study_ID;