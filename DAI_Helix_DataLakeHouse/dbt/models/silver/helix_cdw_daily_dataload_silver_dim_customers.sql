{{
    config( 
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_CDW',
        materilized='incremental',
        alias='DIM_CUSTOMER_1',
        unique_key='C_CODE' 
    )
}}

SELECT  seq_stg_part.nextval as c_custkey, A.C_CODE, A.C_NAME, A.C_ADDRESS, B.n_nationkey as C_nation_key,
        A.C_PHONE, A.C_ACCTBAL, A.C_MKTSEGMENT, A.C_COMMENT, 
        current_timestamp()  as LAST_UPDATE_DATE, 
       {{ get_ts_usr_acname() }} 
FROM 
    {{ ref('helix_bronze_daily_dataload_stg_customers') }} A
    INNER JOIN {{ ref('helix_cdw_daily_dataload_silver_dim_nations') }} B ON B.N_NAME = A.C_NATION_NAME


{% if is_incremental() %}

  WHERE C_MODIFIED_DATE > (select COALESCE(MAX(MODIFIED_DATE), TO_DATE('1900-01-01'))  FROM {{ this }})
  -- AND NOT EXISTS (select 1 from {{this}} B where A.C_code = B.c_code)

{% endif %}