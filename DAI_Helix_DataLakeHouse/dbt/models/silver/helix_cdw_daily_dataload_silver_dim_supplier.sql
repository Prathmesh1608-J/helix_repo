{{
    config( 
        database='DA_HELIX_DB_TEST',
        schema='HELIX_CDW',  
        materialized='incremental',
        alias='DIM_SUPPLIER_1',
        unique_key='S_SUPP_CODE'
    )
}}

SELECT seq_stg_part.nextval as S_SUPPKEY, 
        S_SUPP_CODE, S_SUPP_NAME as S_NAME, S_ADDRESS, B.N_NATIONKEY as S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT, S_MODIFIED_DATE,  
       {{ get_ts_usr_acname() }}
FROM 
    {{ ref('helix_bronze_daily_dataload_stg_suppliers') }} A
    --DA_HELIX_DB_TEST.HELIX_STG.STG_SUPPLIER A
    LEFT JOIN DA_HELIX_DB_TEST.HELIX_CDW.DIM_NATION_1 B ON A.S_NATION_NAME = B.N_NAME

{% if is_incremental() %}

  WHERE S_modified_date > (select coalesce(max(S_MODIFIED_DATE), to_date('1900-01-01'))  from {{ this }})

{% endif %}