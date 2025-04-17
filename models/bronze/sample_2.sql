{{
    config(
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_RAW',
        alias='STG_CUSTOMER1234'
    )
}}

WITH source_data AS (
    SELECT *
    FROM da_helix_db_test.helix_raw.task_run_stats1
    WHERE EXECUTION_RUN_ID >= '{{ var('run_id') }}'

)
 
SELECT *
FROM source_data