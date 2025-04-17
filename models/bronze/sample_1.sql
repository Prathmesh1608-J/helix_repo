{{
    config(
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_RAW',
        alias='STG_CUSTOMER12'
    )
}}

{% set execution_run_id = var('EXECUTION_RUN_ID', none) %}

{% if execution_run_id is none %}
    {{ log("Execution run ID is null", info=True) }}
{% endif %}

SELECT
    CASE
        WHEN {{ execution_run_id | default('NULL') }} IS NOT NULL THEN {{ execution_run_id }} / 1
        ELSE NULL
    END AS EXECUTION_RUN_ID
from da_helix_db_test.helix_raw.task_run_stats1
