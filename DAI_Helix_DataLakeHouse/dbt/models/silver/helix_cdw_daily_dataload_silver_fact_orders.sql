{{
    config( 
        database='DA_HELIX_DB_TEST',
        schema='HELIX_CDW',  
        materialized='incremental',
        alias='FACT_ORDERS_1',
        unique_key='O_ORDER_NUMBER'
    )
}}

SELECT seq_stg_part.nextval as O_ORDERKEY, 
        L_ORDER_NUMBER as O_ORDER_NUMBER, B.C_CUSTKEY AS O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDER_PRIORITY as O_ORDERPRIORITY, O_CLERK, O_SHIP_PRIORITY as O_SHIPPRIORITY, O_COMMENT,
       {{ get_ts_usr_acname() }}
FROM 
    {{ ref('helix_bronze_daily_dataload_stg_order_lineitem_details')}} A
    LEFT JOIN {{ ref('helix_cdw_daily_dataload_silver_dim_customers') }} B ON A.L_CUST_CODE = B.C_NAME


{% if is_incremental() %}

  WHERE L_modified_date > (select coalesce(max(MODIFIED_DATE),to_date('1900-01-01'))  from {{ this }})
  qualify row_number() over (partition by L_ORDER_NUMBER order by L_ORDER_NUMBER) = 1
{% endif %}