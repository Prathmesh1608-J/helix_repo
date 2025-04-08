-- helix_stg_daily_epic_stg_customers.sql

{{
    config(
        enabled=true,
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_STG',
        alias='STG_CUSTOMER',
        unique_key="C_CODE" ,
        materialized="incremental",
        pre_hook=["{{update_pre_data_load_task_stats('helix_poc_proj','RAW_CUSTOMER',123,100001)}}"], 
        post_hook=["{{update_post_data_load_task_stats('helix_poc_stg_proj','STG_CUSTOMER',123,100001)}}"]
    )
}}

SELECT  *, {{ get_ts_usr_acname() }}
FROM 
     {{ source('helix_poc_proj', 'RAW_CUSTOMER') }}
