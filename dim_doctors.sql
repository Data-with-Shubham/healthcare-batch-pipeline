-- ============================================================
-- models/dim_doctors.sql
-- Doctor dimension
-- ============================================================

{{ config(materialized = 'table') }}

WITH deduped AS (

    SELECT
        doctor_id,
        name,
        specialization,
        ROW_NUMBER() OVER (PARTITION BY doctor_id ORDER BY (SELECT NULL)) AS rn
    FROM `{{ var("project_id") }}.{{ var("dataset") }}.dim_doctors`

)

SELECT
    doctor_id,
    name,
    specialization
FROM deduped
WHERE rn = 1
