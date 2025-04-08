
{% macro update_pre_data_load_task_stats(execution_run_id,task_id) %}

        {{ log("Auditing Entries for TASK_RUN_STATS before model run..... ", info=True) }}       

        INSERT INTO {{ source('helix_poc_audit_proj', 'TASK_RUN_STATS') }} -- table details in sources.yml

        -- CTE to fetch the row count of source table
        with src_count_cte as (
            SELECT count(*) as source_count FROM {{ source('helix_poc_proj', 'RAW_PART') }}
        ),
        -- CTE to fetch the task details from TASK_METADATA table
        task_dtls_cte as (
            SELECT distinct task_name as task_name from DA_HELIX_DB_TEST.HELIX_AUDIT.TASK_METADATA
            where task_id= {{ task_id }}
        )
        SELECT 
        {{ execution_run_id }} as execution_run_id, -- run time arg fetched from matillion job
        {{ task_id }} as task_id,
        t2.task_name as task_name,
        current_timestamp as load_start_time,
        t1.source_count as source_count,
        0 as target_count,
        null,
        null,
        'RUNNING' as status,
        null,
        CURRENT_USER() as CREATED_BY,
        CURRENT_DATE() as CREATED_DATE,  
        CURRENT_USER() as MODIFIED_BY,
        CURRENT_DATE() as MODIFIED_DATE 
        FROM src_count_cte t1,task_dtls_cte t2
{% endmacro %}