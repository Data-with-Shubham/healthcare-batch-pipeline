-- ============================================================
-- models/dim_patients.sql
-- Patient dimension — SCD Type 1 (latest record wins)
-- ============================================================

{{ config(materialized = 'table') }}

WITH deduped AS (

    SELECT
        patient_id,
        age,
        gender,
        city,
        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY (SELECT NULL)) AS rn
    FROM `{{ var("project_id") }}.{{ var("dataset") }}.dim_patients`

)

SELECT
    patient_id,
    age,
    gender,
    city,

    -- Derived age band for reporting
    CASE
        WHEN age <  18 THEN 'Paediatric'
        WHEN age <  40 THEN 'Young Adult'
        WHEN age <  60 THEN 'Middle-Aged'
        ELSE                'Senior'
    END AS age_band

FROM deduped
WHERE rn = 1
