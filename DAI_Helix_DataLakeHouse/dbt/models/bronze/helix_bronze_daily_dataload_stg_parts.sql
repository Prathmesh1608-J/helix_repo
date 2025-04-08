--Model to load incremental data from raw_part to stg_part as part of bronze layer
--defining the kind of load
--argumnets to the macros will be received from run time arg in dbt run command
{{
    config(
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_STG',
        materialized='incremental', 
        alias='STG_PART',
        unique_key='p_part_code',
        pre_hook=["{{update_pre_data_load_task_stats(123,100003)}}"], 
        post_hook=["{{update_post_data_load_task_stats(123,100003)}}"]
    )
}}

  SELECT *, '' as P_MFGR,  
    {{ get_ts_usr_acname() }}
  FROM 
     {{ source('helix_poc_proj', 'RAW_PART') }}

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE P_MODIFIED_DATE > (select COALESCE(max(P_MODIFIED_DATE), TO_DATE('1900-01-01')) FROM {{ this }})

{% endif %}