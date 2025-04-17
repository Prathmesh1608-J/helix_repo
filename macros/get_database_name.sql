{% macro get_database_name() %}
    {%- set database_query -%}
		select TARGET_DATABASE_NAME from ANALYTICS.DBT_MKANNA.INGESTION_METADATA_RELATIONAL where lower(TARGET_TABLE_NAME) = '{{this.table}}' 
    {%- endset -%}
    {% set result = run_query(database_query) %}
    {% if execute %}
    {% set tgt_database = result.columns[0].values()[0] | string %}
    {% else %} 
        {% set tgt_database = "ANALYTICS" %}
    {% endif %}
    {{ return(tgt_database) }}
{% endmacro %}
