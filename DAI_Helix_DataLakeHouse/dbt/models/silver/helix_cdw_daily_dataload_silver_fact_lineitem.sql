{{
    config( 
        database='DA_HELIX_DB_TEST',
        schema='HELIX_CDW',  
        materialized='incremental',
        alias='FACT_LINEITEM_1',
        unique_key=['L_ORDERKEY','L_ORDER_LINE_NUMBER']
    )
}}

SELECT  O_ORDERKEY as L_ORDERKEY, 
        L_ORDER_LINE_NUMBER, C.P_PARTKEY AS L_PARTKEY, D.S_SUPPKEY AS L_SUPPKEY,L_ORDER_LINE_NUMBER AS L_LINENUMBER , L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT,
       {{ get_ts_usr_acname() }}
FROM 
    {{ ref('helix_bronze_daily_dataload_stg_order_lineitem_details')}} A
    LEFT JOIN {{ ref('helix_cdw_daily_dataload_silver_fact_orders')}} B on A.L_ORDER_NUMBER = B.O_ORDER_NUMBER
    LEFT JOIN {{ ref('helix_cdw_daily_dataload_silver_dim_parts') }} C ON A.L_PART_CODE = C.P_PART_CODE
    LEFT JOIN {{ ref('helix_cdw_daily_dataload_silver_dim_supplier')}} D on A.L_SUPP_CODE = D.S_SUPP_CODE


{% if is_incremental() %}

  WHERE L_modified_date > (select coalesce(max(MODIFIED_DATE),to_date('1900-01-01'))  from {{ this }})
  
{% endif %}