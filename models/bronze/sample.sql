{{
    config(
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_RAW',
        alias='STG_CUSTOMER123'
    )
}}
 
{% set execution_run_id = var('EXECUTION_RUN_ID', none) %}
 
{% if execution_run_id is not none %}
    SELECT  
    EXECUTION_RUN_ID/1 as EXECUTION_RUN_ID
    from da_helix_db_test.helix_raw.task_run_stats1
{% else %}
    {{ exceptions.raise_compiler_error("Invalid. Got: null") }}
{% endif %}
 