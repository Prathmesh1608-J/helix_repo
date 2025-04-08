{% macro get_ts_usr_acname() %}
    '' as BATCH_ID,
	'' as JOB_ID,
	'' as EXECUTION_ID,
	'' as INSTANCE_ID,
    CURRENT_USER() as CREATED_BY,
    CURRENT_DATE() as CREATED_DATE,  
    CURRENT_USER() as MODIFIED_BY,
    CURRENT_DATE() as MODIFIED_DATE 
{% endmacro %}