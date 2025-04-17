{% macro log_dbt_results(results) %}
    {{ log("Starting the execution for macro", info=True) }}
    
    {%- if execute -%}
        {%- set parsed_results = parse_dbt_results(results) -%}
        {{ log(parsed_results, info=True) }}

        {%- for parsed_result_list in parsed_results -%}
        {{ log(parsed_result_list, info=True) }}
            {%- if parsed_results | length  > 0 -%}
                {%- set task_name = parsed_results[0].get('name') -%}  
                {%- set ERROR_MESSAGE = parsed_result_list.get('ERROR_MSG') -%}            
                {{ log("task_name: " + task_name, info=True) }}
                {{ log("ERROR_MESSAGE: " + ERROR_MESSAGE, info=True) }}
            {%- endif -%}
        


            {% set results = run_query("SELECT EXECUTION_RUN_ID, TASK_ID FROM DA_HELIX_DB_TEST.HELIX_RAW.TASK_RUN_STATS where TASK_NAME = '" ~ task_name ~ "'") %}
            {{ log(results, info=True) }}
            {%- if results and results.rows|length > 0 -%}
                {%- set EXECUTION_RUN_ID = results.rows[0][0] -%}
                {%- set TASK_ID = results.rows[0][1] -%}
            {%- else -%}
                {%- set EXECUTION_RUN_ID = 0 -%}
                {%- set TASK_ID = 0 -%}
            {%- endif -%}
            
            {{ log(EXECUTION_RUN_ID, info=True) }}
            {{ log(TASK_ID, info=True) }}
            {{ log('TASK_ID Available', info=True) }}

            {% set result_run_stats = run_query("SELECT JOB_ID FROM DA_HELIX_DB_TEST.HELIX_RAW.JOB_RUN_STATS where EXECUTION_RUN_ID = " ~ EXECUTION_RUN_ID) %}
            {%- if result_run_stats and result_run_stats.rows|length > 0 -%}
                {%- set JOB_ID = result_run_stats.rows[0][0] -%}
            {%- else -%}
                {%- set JOB_ID = 0 -%}
            {%- endif -%}

            {% set result_batch_run_stats = run_query("SELECT BATCH_ID FROM DA_HELIX_DB_TEST.HELIX_RAW.BATCH_RUN_STATS where EXECUTION_RUN_ID = " ~ EXECUTION_RUN_ID) %}
            {%- if result_batch_run_stats and result_batch_run_stats.rows|length > 0 -%}
                {%- set BATCH_ID = result_batch_run_stats.rows[0][0] -%}
            {%- else -%}
                {%- set BATCH_ID = 0 -%}
            {%- endif -%}

            {% set insert_dbt_results_query -%}

                insert into {{ source('helix_poc_proj', 'ERROR_STATS') }}
                    (
                    EXECUTION_RUN_ID,
                    ERROR_ID,
                    JOB_ID,
                    TASK_ID,
                    BATCH_ID,
                    ERROR_MSG,
                    ERROR_TYPE
                ) SELECT 
                    {{ EXECUTION_RUN_ID }},
                    2,
                    {{ JOB_ID }},
                    {{ TASK_ID }},
                    {{ BATCH_ID }},
                    '{{ ERROR_MESSAGE }}',
                    NULL     
                    WHERE '{{ ERROR_MESSAGE }}' LIKE '%Error%'   
                    OR '{{ ERROR_MESSAGE }}' LIKE '%fail%'   
            {%- endset -%}
            {%- do run_query(insert_dbt_results_query) -%}
            {{- "," if not loop.last else "" -}}
        {%- endfor -%}
    {%- endif -%}
    -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
    {{ return ('') }}
{% endmacro %}