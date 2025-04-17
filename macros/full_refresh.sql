{% macro full_refresh() %}
    {% set materialization_type = get_materialization() %}
    {{ log('Materialization type: ' ~ materialization_type, info=True) }}
    {% if execute %}
        {% if materialization_type == 'full_refresh' %}
            TRUNCATE TABLE {{ this }};
            {{ log('Table truncated', info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}
