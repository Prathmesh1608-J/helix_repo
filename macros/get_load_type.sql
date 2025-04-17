{% macro get_load_type(table_name) %}
{%- set query %}
        SELECT 
        MATERIALIZATION_TYPE 
        FROM DA_HELIX_DB_TEST.HELIX_RAW.TASK_RUN_STATS
        WHERE TASK_NAME = '{{ this.table }}'
        LIMIT 1
{%- endset %}

{%- set results = run_query(query) %}
{%- set load_type = load_type(results) %}

{{ load_type }}
{% endmacro %}
