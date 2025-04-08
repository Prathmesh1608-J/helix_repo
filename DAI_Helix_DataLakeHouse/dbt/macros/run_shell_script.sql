{% macro run_shell_script(script_path) %}
{% do run_query("SELECT system('{}')".format(script_path)) %}
{% endmacro %}