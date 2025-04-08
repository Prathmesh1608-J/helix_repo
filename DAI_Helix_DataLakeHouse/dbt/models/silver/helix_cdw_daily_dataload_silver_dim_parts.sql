{{
    config( 
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_CDW',  
        materialized='incremental',
        alias='DIM_PART_1',
        unique_key='p_part_code'
    )
}}

WITH ct_max_dt AS (
    SELECT COALESCE(max(p_MODIFIED_DATE), TO_DATE('1900-01-01')) AS max_dt FROM {{ this }}
)

SELECT seq_stg_part.nextval as p_partkey, 
        P_PART_CODE, P_MFGR_CODE, P_NAME, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT, P_MODIFIED_DATE , 
       '' as p_mfgr,  {{ get_ts_usr_acname() }}
FROM 
    {{ ref('helix_bronze_daily_dataload_stg_parts')}}


{% if is_incremental() %}

  WHERE p_modified_date > (SELECT max_dt FROM ct_max_dt)

{% endif %}