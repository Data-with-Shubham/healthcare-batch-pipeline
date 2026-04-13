-- ============================================================
-- models/fact_admissions.sql
-- Fact table: one row per hospital admission
-- ============================================================

{{
  config(
    materialized = 'table',
    partition_by = {
      "field": "admission_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by = ["department_id", "doctor_id"]
  )
}}

WITH base AS (

    SELECT
        fa.admission_id,
        fa.patient_id,
        fa.doctor_id,
        fa.department_id,
        fa.treatment_id,
        fa.admission_date,
        fa.discharge_date,
        fa.length_of_stay,
        fa.readmission_flag,
        fa.ingested_at,

        -- Derived: flag if prior admission exists within 30 days
        LAG(fa.admission_date) OVER (
            PARTITION BY fa.patient_id
            ORDER BY fa.admission_date
        ) AS prev_admission_date

    FROM `{{ var("project_id") }}.{{ var("dataset") }}.fact_admissions` fa

),

enriched AS (

    SELECT
        admission_id,
        patient_id,
        doctor_id,
        department_id,
        treatment_id,
        admission_date,
        discharge_date,
        length_of_stay,

        -- Recalculate readmission based on actual data
        CASE
            WHEN prev_admission_date IS NOT NULL
             AND DATE_DIFF(admission_date, prev_admission_date, DAY) <= 30
            THEN TRUE
            ELSE FALSE
        END                         AS readmission_flag,

        ingested_at

    FROM base

)

SELECT * FROM enriched

-- dbt data quality tests (defined in schema.yml)
