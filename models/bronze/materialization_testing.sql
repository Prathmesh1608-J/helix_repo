{% set materialization_type = get_materialization() %}
{{ log('Materialization type: ' ~ materialization_type, info=True) }}

{{ config(
    target_database='DA_HELIX_DB_TEST',
    schema='HELIX_RAW',
    alias='materialization_testing',
    materialized='incremental'
) }}

{{ log("Materialized value: " ~ config.get('materialized'), info=True) }}
{{ log("pre_hook value: " ~ config.get('pre_hook'), info=True) }}

SELECT  *, {{ get_ts_usr_acname() }} 
FROM DA_HELIX_DB_TEST.HELIX_RAW.TASK_RUN_STATS

{% if materialization_type == 'incremental' %}
    -- Incremental load logic
    WHERE EXECUTION_RUN_ID > (SELECT MAX(EXECUTION_RUN_ID) FROM {{ this }})
{% endif %}
