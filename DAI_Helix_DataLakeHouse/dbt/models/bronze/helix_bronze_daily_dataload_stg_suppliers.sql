{{
    config(
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_STG',
        alias='STG_SUPPLIER',
        unique_key="S_SUPP_NAME",
        materialized='incremental'  
    )
}}

SELECT  S_SUPP_CODE, S_SUPP_NAME, S_ADDRESS, S_NATION_NAME, S_PHONE, S_ACCTBAL, S_COMMENT, S_MODIFIED_DATE,
        {{ get_ts_usr_acname() }}
FROM 
     {{ source('helix_poc_proj', 'RAW_SUPPLIER') }}

{% if is_incremental() %}

  WHERE S_MODIFIED_DATE > (select coalesce(max(S_MODIFIED_DATE),'1900-01-01')  from {{ this }})

{% endif %}