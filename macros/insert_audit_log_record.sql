{% macro insert_audit_log_record() %}

{% set updated_at = model.config.snapshot_meta_column_names.dbt_updated_at | default("dbt_updated_at") %}
{% set valid_from = model.config.snapshot_meta_column_names.dbt_valid_from | default("dbt_valid_from") %}
{% set valid_to = model.config.snapshot_meta_column_names.dbt_valid_to | default("dbt_valid_to") %}

insert into {{ model.database }}.{{model.schema}}.AUDIT_LOG (query_tag,invocation_id,execution_timestamp, model_name,rows_inserted, rows_updated,rows_deleted)


with inserts
as (
select 1 as id, 
count(*) as rows_inserted
from {{ this }}
where {{ updated_at }} > '{{run_started_at}}' and {{valid_to}} is null
and composite_key not in (select distinct composite_key from {{ this }} where {{ valid_to}} is null )
),

update as (
select 1 as id, count(*) as rows_updated
from {{ this }}
where {{ valid_to }} > '{{run_started_at}}' 
and composite_key in (select distinct composite_key from {{ this }} where {{ valid_to}} is null )
),

delete as (
select 1 as id, count(*) as rows_deleted
from {{ this }}
where {{ valid_to }} > '{{run_started_at}}' 
and composite_key not in (select distinct composite_key from {{ this }} where {{ valid_to}} is null )
)


select
'{{ query_tag }}' as query_tag,
'{{ invocation_id }}' as invocation_id,
'{{ run_started_at }}' as execution_timestamp,
'{{ this}}' as model_name,
i.rows_inserted,
u.rows_updated,
d.rows_deleted
from inserts i
left join updates u on i.id = u.id
left join deletes d on i.id = d.id;
{% endmacro %}
