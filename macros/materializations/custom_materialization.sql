{% materialization custom_materialization, default -%}

{%- set table_name = '{{ this.table }}' -%}
{%- set load_type = get_load_type(table_name) -%}

{%- if load_type == 'incremental' -%}

-- Incremental logic
{{ log('Running incremental load', info=True) }}

{%- if is_incremental() -%}
-- Your incremental logic here
{%- else -%}
-- Initial load logic for incremental
{%- endif -%}

{%- elif load_type == 'full_refresh' -%}

-- Full refresh logic
{{ log('Running full refresh load', info=True) }}
{{ run_query('TRUNCATE TABLE {{ this }}') }}

{%- else -%}
{{ exceptions.raise_compiler_error("Invalid load_type specified: " ~ load_type) }}
{%- endif -%}

{%- endmaterialization -%}
{% materialization truncate_table, adapter='default' %}

{# This materialization will truncate the table and then load new data #}

{{ log('Starting truncate_table materialization', info=True) }}

-- Define the target relation
{% set target_relation = this %}

-- Check if the table exists
{% if not is_incremental() %}
    {% if adapter.get_relation(target_relation.database, target_relation.schema, target_relation.identifier) is not none %}
        {{ log('Table exists, truncating table: ' ~ target_relation, info=True) }}
        -- Truncate the table if it exists
        {{ adapter.execute("TRUNCATE TABLE " ~ target_relation) }}
    {% else %}
        {{ log('Table does not exist, creating table: ' ~ target_relation, info=True) }}
        -- Create the table if it does not exist
        {{ adapter.create_table_as(target_relation, sql) }}
    {% endif %}
{% endif %}

{{ log('Inserting data into table: ' ~ target_relation, info=True) }}
-- Insert new data into the table
{{ adapter.insert_into(target_relation, sql) }}

{{ log('Completed truncate_table materialization', info=True) }}

{% endmaterialization %}
