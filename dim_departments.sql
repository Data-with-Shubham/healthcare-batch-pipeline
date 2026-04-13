-- ============================================================
-- models/dim_departments.sql
-- Department dimension
-- ============================================================

{{ config(materialized = 'table') }}

WITH deduped AS (

    SELECT
        department_id,
        department_name,
        ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY (SELECT NULL)) AS rn
    FROM `{{ var("project_id") }}.{{ var("dataset") }}.dim_departments`

)

SELECT
    department_id,
    department_name
FROM deduped
WHERE rn = 1
