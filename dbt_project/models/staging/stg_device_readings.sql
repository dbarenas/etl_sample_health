-- dbt_project/models/staging/stg_device_readings.sql

WITH source AS (
    SELECT * FROM {{ source('public', 'device_readings') }}
)

SELECT
    id AS device_reading_id, -- Renaming for clarity
    patient_id, -- This should match the patient_id from stg_patients (e.g., patients.id)
    timestamp AS reading_timestamp,
    glucose,
    systolic_bp,
    diastolic_bp,
    weight
FROM
    source
WHERE
    patient_id IS NOT NULL -- Basic data quality filter, if patient_id is crucial for joining
    AND timestamp IS NOT NULL -- Basic data quality filter
    -- Add other filters if necessary, e.g., ensuring numeric fields are valid if not handled by source tests
