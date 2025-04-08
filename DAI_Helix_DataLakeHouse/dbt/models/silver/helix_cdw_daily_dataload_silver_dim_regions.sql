{{
    config( 
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_CDW',
        alias='DIM_REGION_1',
        materialized='incremental',
        unique_key='r_name' 
    )
}}

SELECT  seq_stg_part.nextval as r_regionkey,  
        A.c_region_name as r_name, A.c_region_comment as r_comment,  
        current_timestamp()  as LAST_UPDATE_DATE,
       {{ get_ts_usr_acname() }} 
FROM 
    {{ ref('helix_bronze_daily_dataload_stg_customers')}} A
WHERE 
    NOT EXISTS (select 1 from {{this}} B where B.r_name = A.c_region_name)
    AND C_MODIFIED_DATE > (select COALESCE(max(MODIFIED_DATE), to_date('1900-01-01'))  from {{ this }})
    QUALIFY row_number() over(partition by A.c_region_name order by A.c_region_name) =1
    