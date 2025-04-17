{{ config(
    target_database='DA_HELIX_DB_TEST',
    schema='HELIX_RAW',
    alias='materialization_testing2',
    materialized='incremental',
    unique_key='TASK_ID'
) }}

{% set is_inc = get_incremental_flag('my_model') %}
{{ log('Materialization type:' ~ is_inc, info=True) }}
{% if is_inc == False %}
    {{ log('Truncating table', info=True) }}
    {{ full_refresh(this) }}
{% endif %}


{% if is_inc == True %}
    -- Incremental logic
    {{ log('Running in incremental mode', info=True) }}
    SELECT  *,    {{ get_ts_usr_acname() }} FROM     DA_HELIX_DB_TEST.HELIX_RAW.TASK_RUN_STATS  
    WHERE EXECUTION_RUN_ID > (SELECT MAX(EXECUTION_RUN_ID) FROM {{ this }})
{% else %}
    -- Full refresh logic
    {{ log('Running in FULL REFRESH mode', info=True) }}
    SELECT  *,    {{ get_ts_usr_acname() }} FROM     DA_HELIX_DB_TEST.HELIX_RAW.TASK_RUN_STATS
{% endif %}
