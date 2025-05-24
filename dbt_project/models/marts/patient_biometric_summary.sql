-- dbt_project/models/marts/patient_biometric_summary.sql

WITH patients AS (
    SELECT * FROM {{ ref('stg_patients') }}
),

device_readings AS (
    SELECT * FROM {{ ref('stg_device_readings') }}
)

SELECT
    p.patient_id,
    p.patient_name, -- Include patient name for easier identification in the summary
    p.date_of_birth,
    p.gender,
    p.sex,

    COUNT(dr.device_reading_id) AS total_readings,

    MIN(dr.glucose) AS min_glucose,
    MAX(dr.glucose) AS max_glucose,
    AVG(dr.glucose) AS avg_glucose,

    MIN(dr.systolic_bp) AS min_systolic_bp,
    MAX(dr.systolic_bp) AS max_systolic_bp,
    AVG(dr.systolic_bp) AS avg_systolic_bp,

    MIN(dr.diastolic_bp) AS min_diastolic_bp,
    MAX(dr.diastolic_bp) AS max_diastolic_bp,
    AVG(dr.diastolic_bp) AS avg_diastolic_bp,

    MIN(dr.weight) AS min_weight,
    MAX(dr.weight) AS max_weight,
    AVG(dr.weight) AS avg_weight,
    
    MIN(dr.reading_timestamp) AS first_reading_timestamp,
    MAX(dr.reading_timestamp) AS last_reading_timestamp

FROM
    patients p
JOIN
    device_readings dr ON p.patient_id = dr.patient_id
GROUP BY
    p.patient_id,
    p.patient_name,
    p.date_of_birth,
    p.gender,
    p.sex
ORDER BY
    p.patient_name, 
    p.patient_id -- Ensure consistent ordering
