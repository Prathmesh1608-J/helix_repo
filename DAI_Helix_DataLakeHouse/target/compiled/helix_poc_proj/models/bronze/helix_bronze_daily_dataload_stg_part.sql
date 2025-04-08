--insert into task stats based task id, status =RUNNING


--defining the kind of load


  SELECT * , 
    '' as BATCH_ID,
	'' as JOB_ID,
	'' as EXECUTION_ID,
	'' as INSTANCE_ID,
    CURRENT_USER() as CREATED_BY,
    CURRENT_DATE() as CREATED_DATE,  
    CURRENT_USER() as MODIFIED_BY,
    CURRENT_DATE() as MODIFIED_DATE 

from 
     DA_HELIX_DB_TEST.HELIX_RAW.RAW_PART

  -- this filter will only be applied on an incremental run
    where P_MODIFIED_DATE > (select max(P_MODIFIED_DATE) from DA_HELIX_DB_TEST.HELIX_STG.STG_PART)
