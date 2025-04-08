{% macro log_dbt_results(results) %}
    {%- if execute -%}
        {%- set parsed_results = parse_dbt_results(results) -%}
        {%- if parsed_results | length  > 0 -%}
            {% set insert_dbt_results_query -%}
                insert into {{ source('COMMON_DB_LOG', 'ERROR_STATS') }}
                    (
                        TASK_ID,
                        TASK_NAME,
                        LOAD_START_TIME,
                        LOAD_END_TIME,
                        DURATION_IN_SECONDS,
                        STATUS,
                        ERROR_MSG
                ) values
                    {%- for parsed_result_dict in parsed_results -%}
                        (
                            '{{ parsed_result_dict.get('TASK_ID') }}',
                            '{{ parsed_result_dict.get('TASK_NAME') }}',
                            '{{ parsed_result_dict.get('LOAD_START_TIME') }}',
                            '{{ parsed_result_dict.get('LOAD_END_TIME') }}',
                            '{{ parsed_result_dict.get('DURATION_IN_SECONDS') }}',
                            '{{ parsed_result_dict.get('STATUS') }}',
                            '{{ parsed_result_dict.get('ERROR_MSG') }}'
                        ) {{- "," if not loop.last else "" -}}
                    {%- endfor -%}
            {%- endset -%}
            {%- do run_query(insert_dbt_results_query) -%}
        {%- endif -%}
    {%- endif -%}
    -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
    {{ return ('') }}
{% endmacro %}
