-- ============================================================
-- models/dim_treatments.sql
-- Treatment dimension with cost bands
-- ============================================================

{{ config(materialized = 'table') }}

WITH deduped AS (

    SELECT
        treatment_id,
        treatment_name,
        treatment_cost,
        ROW_NUMBER() OVER (PARTITION BY treatment_id ORDER BY (SELECT NULL)) AS rn
    FROM `{{ var("project_id") }}.{{ var("dataset") }}.dim_treatments`

)

SELECT
    treatment_id,
    treatment_name,
    treatment_cost,

    -- Cost band for Power BI slicers
    CASE
        WHEN treatment_cost <  5000  THEN 'Low'
        WHEN treatment_cost < 20000  THEN 'Medium'
        WHEN treatment_cost < 50000  THEN 'High'
        ELSE                              'Critical'
    END AS cost_band

FROM deduped
WHERE rn = 1
