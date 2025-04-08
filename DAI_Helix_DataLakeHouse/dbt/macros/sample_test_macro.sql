{% macro my_macro(column_name,table_name) %}
          SELECT {{ column_name }} FROM {{ table_name }} WHERE NAME='{{this}}' and 
          GENERATED_AT=(SELECT MAX(GENERATED_AT) FROM {{ table_name }} WHERE NAME='{{this}}')
        {% endmacro %}