-- macros/get_incremental_flag.sql
{% macro get_incremental_flag(model_name) %}
    {% set query %}
        SELECT is_incrementl
        FROM DA_HELIX_DB_TEST.HELIX_RAW.task_metadata_table
        WHERE model_name = '{{ this.table }}'
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% set is_incremental = results.columns[0].values()[0] %}
        {{ log("is_incremental: " ~ is_incremental, info=True) }}
    {% else %}
        {% set is_incremental = false %}
    {% endif %}
    
    {{ is_incremental }}
{% endmacro %}
