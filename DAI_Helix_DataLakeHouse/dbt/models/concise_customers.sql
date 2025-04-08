{{ log(var('tgt_database'), info=True) }}
{{ config(materialized='table',database=var('tgt_database')) }}
select id as customer_id, first_name, last_name

from raw.jaffle_shop.customers
