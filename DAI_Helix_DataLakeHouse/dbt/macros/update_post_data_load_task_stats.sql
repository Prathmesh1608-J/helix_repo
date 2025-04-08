
{% macro update_post_data_load_task_stats(execution_run_id,task_id) %}

        {{ log("Auditing Entries for TASK_RUN_STATS after model run..... ", info=True) }}       

        UPDATE {{ source('helix_poc_audit_proj', 'TASK_RUN_STATS') }} -- table details in sources.yml

        SET target_count = t1.target_count,
        LOAD_END_TIME  = current_timestamp(),
        DURATION_IN_SECONDS = DATEDIFF(SECOND, LOAD_START_TIME::timestamp, LOAD_END_TIME::timestamp),
        STATUS = 'COMPLETED',
        ERROR_MSG = 'NA', 
        MODIFIED_BY = CURRENT_USER(),
        MODIFIED_DATE = CURRENT_DATE()
        FROM -- CTE to fetch the row count of target table
        (
            with tgt_count_cte as (
                SELECT count(*) as target_count FROM {{ source('helix_poc_bronze_proj', 'STG_PART') }}
            )
            select * from tgt_count_cte
         ) as t1
         WHERE task_id= {{ task_id }} and EXECUTION_RUN_ID= {{ execution_run_id }};
{% endmacro %}