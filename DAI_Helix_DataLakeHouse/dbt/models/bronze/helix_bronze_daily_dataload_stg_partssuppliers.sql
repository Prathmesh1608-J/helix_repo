{{
    config(
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_STG',
        alias='STG_PARTSSUPPLIER',
        unique_key=["PS_PART_CODE","PS_SUPP_CODE"],
        materialized='incremental'  
    )
}}

SELECT  PS_PART_CODE, PS_SUPP_CODE, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT,PS_MODIFIED_DATE,
        {{ get_ts_usr_acname() }}
FROM 
     {{ source('helix_poc_proj', 'RAW_PARTSSUPPLIER') }}

{% if is_incremental() %}

  WHERE PS_MODIFIED_DATE > (select coalesce(max(PS_MODIFIED_DATE), to_date('1900-01-01'))  from {{ this }})

{% endif %}