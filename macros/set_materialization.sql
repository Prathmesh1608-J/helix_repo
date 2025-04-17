-- macros/get_materialization_strategy.sql
{% macro get_materialization_strategy() %}
    {% set result = run_query("SELECT MATERIALIZATION_TYPE FROM DA_HELIX_DB_TEST.HELIX_RAW.TASK_RUN_STATS WHERE TASK_NAME = 'materialized_testing'") %}
    {% if result %}
        {% set materialization = result.columns[0].values()[0] %}
    {% else %}
        {% set materialization = 'incremental' %}
    {% endif %}
    {{ return(materialization) }}
{% endmacro %}
