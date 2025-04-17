{{
    config(
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_RAW',
        alias='RUN_RESULT_ERROR',
        unique_key="C_CODE",
        materialized='incremental'
    )
}}

with error_messages as (
    select
        1 as error_data
)

select
error_data
from
    error_messages
