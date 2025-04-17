{{ 
    config(
    target_database='DA_HELIX_DB_TEST',
    schema='HELIX_RAW',
    alias='materialization_testing1',
    materialized='incremental'
) }}
-- SQL query
SELECT  *,    {{ get_ts_usr_acname() }} FROM     DA_HELIX_DB_TEST.HELIX_RAW.TASK_RUN_STATS

{% if is_incremental %} 
  {{ log('Running in incremental mode', info=True) }}
  WHERE EXECUTION_RUN_ID > (SELECT MAX(EXECUTION_RUN_ID) FROM {{ this }})
{% endif %}