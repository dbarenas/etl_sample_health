-- dbt_project/models/staging/stg_patients.sql

WITH source AS (
    SELECT * FROM "etl_data"."public"."patients"
)

SELECT
    id AS patient_id, -- Renaming for clarity in downstream models if desired, or keep as 'id'
    name AS patient_name,
    dob AS date_of_birth,
    gender,
    sex
    -- email, phone, address can be added if needed for analytics
    -- For biometric analytics, these might not be immediately necessary
FROM
    source