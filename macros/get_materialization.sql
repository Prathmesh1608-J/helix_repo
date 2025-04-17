{% macro get_materialization() %}
    {% if execute %}
        {% set materialization %}
            SELECT DISTINCT
            MATERIALIZATION_TYPE 
            FROM DA_HELIX_DB_TEST.HELIX_RAW.TASK_RUN_STATS1
            WHERE TASK_NAME = '{{ this.table }}'
        {% endset %}
    
        {% set results = run_query(materialization) %}
        
        {% if results and results.columns[0].values() %}
            {% set mat_type = results.columns[0].values()[0] %}
            {{ log('Materialization type from query: ' ~ mat_type, info=True) }}
            
            {% if mat_type == 'incremental' %}
                {% set materialization_type = 'incremental' %}
            {% else %}
                {% set materialization_type = 'full_refresh' %}
            {% endif %}
        {% else %}
            {{ log('No materialization type found, defaulting to full_refresh', info=True) }}
            {% set materialization_type = 'full_refresh' %}
        {% endif %}
        
        {{ return(materialization_type) }}
    {% endif %}
{% endmacro %}
