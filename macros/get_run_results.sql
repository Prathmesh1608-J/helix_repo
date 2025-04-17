{% macro fetch_error_messages() %}
    -- Create a list of parsed results
    {%- set parsed_results = [] %}
    -- Flatten results and add to list
    {{ print(results) }}
 
    {% for run_result in results %}
        -- Convert the run result object to a simple dictionary
        {% set run_result_dict = run_result.to_dict() %}
        -- Get the underlying dbt graph node that was executed
        {% set node = run_result_dict.get('node') %}
 
        {% set rows_affected = run_result_dict.get('adapter_response', {}).get('rows_affected', 0) %}
        {%- if not rows_affected -%}
            {% set rows_affected = 0 %}
        {%- endif -%}
 
        {% set parsed_result_dict = {
                'ERROR_MSG': run_result_dict.get('message')|replace("\n", " -")|replace("'", '"')
                }%}
        {% do parsed_results.append(parsed_result_dict) %}
    {% endfor %}
    {{ return(parsed_results) }}
{% endmacro %}