{{
    config( 
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_CDW',
        alias='DIM_NATION_1',
        unique_key='n_name' ,
        materialized='incremental'
    )
}}

SELECT 
       seq_stg_part.nextval as n_nationkey,
       B.r_regionkey as n_regionkey , 
        A.c_nation_name as n_name, A.c_nation_comment as n_comment,  
        current_timestamp()  as LAS_UPDATE_DATE,
       {{ get_ts_usr_acname() }} 
FROM 
    {{ ref('helix_bronze_daily_dataload_stg_customers') }} A
    INNER JOIN {{ ref('helix_cdw_daily_dataload_silver_dim_regions')}} B ON A.c_region_name = b.r_name
    QUALIFY row_number() over(partition by c_nation_name order by c_nation_name) =1 
    AND NOT EXISTS (SELECT 1 FROM {{this}} C WHERE A.c_nation_name = C.n_name)
