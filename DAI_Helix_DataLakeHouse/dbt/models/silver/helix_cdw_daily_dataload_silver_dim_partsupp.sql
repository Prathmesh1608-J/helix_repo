{{
    config( 
        database='DA_HELIX_DB_TEST',
        schema='HELIX_CDW',  
        materialized='incremental',
        alias='DIM_PARTSUPP_1',
        unique_key=['PS_PARTKEY','PS_SUPPKEY']
    )
}}

SELECT B.P_PARTKEY as PS_PARTKEY, C.S_SUPPKEY as PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT, PS_MODIFIED_DATE,  
       {{ get_ts_usr_acname() }}
FROM 
    {{ ref('helix_bronze_daily_dataload_stg_partssuppliers') }} A
   --DA_HELIX_DB_TEST.HELIX_STG.STG_PARTSSUPPLIER A
    LEFT JOIN DA_HELIX_DB_TEST.HELIX_CDW.DIM_PART_1 B ON A.PS_PART_CODE = B.P_PART_CODE
    LEFT JOIN DA_HELIX_DB_TEST.HELIX_CDW.DIM_SUPPLIER_1 C ON A.PS_SUPP_CODE = C.S_SUPP_CODE


{% if is_incremental() %}

  WHERE PS_modified_date > (select coalesce(max(PS_MODIFIED_DATE), to_date('1900-01-01'))  from {{ this }})

{% endif %}