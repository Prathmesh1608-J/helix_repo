{{
    config(
        target_database='DA_HELIX_DB_TEST',
        schema='HELIX_STG',
        alias='STG_ORDER_LINEITEM_DETAILS',
        unique_key=["L_ORDER_NUMBER","L_ORDER_LINE_NUMBER"],
        materialized='incremental'  
    )
}}

SELECT  L_ORDER_NUMBER, L_ORDER_LINE_NUMBER, L_CUST_CODE, L_PART_CODE, L_SUPP_CODE, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDER_PRIORITY, O_CLERK, O_SHIP_PRIORITY, O_COMMENT, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, L_MODIFIED_DATE,
        {{ get_ts_usr_acname() }}
FROM 
     {{ source('helix_poc_proj', 'RAW_ORDER_LINEITEM_DETAILS') }}

{% if is_incremental() %}

  WHERE L_MODIFIED_DATE > (select coalesce(max(L_MODIFIED_DATE),to_date('1900-01-01'))  from {{ this }})

{% endif %}