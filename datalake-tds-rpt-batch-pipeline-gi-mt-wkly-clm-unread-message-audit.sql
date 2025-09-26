
SET TIMEZONE = 'Singapore';	

create table #temp_src_record_eff_from_date  as
SELECT 
	'EBGI-TMPOFFICER' AS source_data_set, MIN(refd) AS src_record_eff_from_date 
FROM (
	SELECT MAX(record_eff_from_date) AS refd FROM tl_ebgi_def.tb_t_user_hist WHERE active_record_ind = 'Y'
	UNION
	SELECT MAX(record_eff_from_date) AS refd FROM tl_ebgi_def.tb_t_pty_indi_hist WHERE active_record_ind = 'Y'
	UNION
	SELECT MAX(record_eff_from_date) AS refd FROM tl_ebgi_def.tb_t_ptyr_hist WHERE active_record_ind = 'Y'
	UNION
	SELECT MAX(record_eff_from_date) AS refd FROM tl_ebgi_def.tb_t_ptyr_rela_hist WHERE active_record_ind = 'Y'
	UNION
	SELECT MAX(record_eff_from_date) AS refd FROM tl_ebgi_def.tb_t_pty_employee_hist WHERE active_record_ind = 'Y'
	)
UNION 
SELECT 
	'EBGI-TMPMESSAGE' AS source_data_set, MIN(refd) AS src_record_eff_from_date 
FROM (
	SELECT MAX(record_eff_from_date) AS refd FROM tl_ebgi_def.tb_t_clm_message_hist WHERE active_record_ind = 'Y'
	UNION
	SELECT MAX(record_eff_from_date) AS refd FROM tl_ebgi_def.tb_t_clm_case_hist WHERE active_record_ind = 'Y'
	UNION
	SELECT MAX(record_eff_from_date) AS refd FROM tl_ebgi_def.tb_t_clm_object_hist WHERE active_record_ind = 'Y'
	UNION
	SELECT MAX(record_eff_from_date) AS refd FROM tl_ebgi_def.tb_t_clm_siu_file_hist WHERE active_record_ind = 'Y'
	)

;

INSERT INTO cl_rpt_stg_temp.ctrl_audit
SELECT
	'cl_rpt_stg' AS tgt_schema,
	'tb_rpt_gi_mt_wkly_clm_unread_message' AS tgt_table,
	'EBGI' AS tgt_source_app_code,
	t1.source_data_set AS tgt_source_data_set,
	t1.src_record_eff_from_date,
	GETDATE() AS data_pipeline_run_date
FROM
	#temp_src_record_eff_from_date t1
;

DROP TABLE IF EXISTS #temp_src_record_eff_from_date;

COMMIT;

--DQ Audit
create table #tb_t_user_hist AS
select
    user_id,
	party_id, 
    dept_id,
    party_role,
    user_name,
    real_name,	
	record_eff_from_date,
	record_eff_to_date,
	active_record_ind,
	dml_ind
	from
	(select
        user_id,
	    party_id,
		dept_id,
		party_role,
		user_name,
        real_name,
		record_eff_from_date,
	    record_eff_to_date,
	    active_record_ind,
	    dml_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_user_hist
		)
WHERE
	rnk = 1
	AND active_record_ind = 'Y';



create table #tb_t_pty_indi_hist AS
select
    pty_id,
	record_eff_from_date,
	record_eff_to_date,
	active_record_ind,
	dml_ind
	from
	(select
        pty_id,
		record_eff_from_date,
	    record_eff_to_date,
	    active_record_ind,
	    dml_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_pty_indi_hist
		)
WHERE
	rnk = 1
	AND active_record_ind = 'Y';
	
create table #tb_t_ptyr_hist AS
select
    pty_id,
    ptyr_id,
    ptyr_type,
    ptyr_status,
	record_eff_from_date,
	record_eff_to_date,
	active_record_ind,
	dml_ind
	from
	(select
        pty_id,
		ptyr_id,
		ptyr_type,
        ptyr_status,
		record_eff_from_date,
	    record_eff_to_date,
	    active_record_ind,
	    dml_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_ptyr_hist
		)
WHERE
	rnk = 1
	AND active_record_ind = 'Y';

create table #tb_t_ptyr_rela_hist AS
select
    from_ptyr_id,
    to_ptyr_id,
    relation_type,
    status,
    record_eff_from_date,
    record_eff_to_date,
    active_record_ind,
    dml_ind	
	from
	(select
        from_ptyr_id,
		to_ptyr_id,
		relation_type,
		status,
		record_eff_from_date,
	    record_eff_to_date,
	    active_record_ind,
	    dml_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_ptyr_rela_hist
		)
WHERE
	rnk = 1
	AND active_record_ind = 'Y';

create table #tb_t_pty_employee_hist AS
select
    ptyr_id,
	employee_status,
	record_eff_from_date,
	record_eff_to_date,
	active_record_ind,
	dml_ind
	from
	(select
        ptyr_id,
		employee_status,
		record_eff_from_date,
	    record_eff_to_date,
	    active_record_ind,
	    dml_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_pty_employee_hist
		)
WHERE
	rnk = 1
	AND active_record_ind = 'Y';

create table #tb_t_clm_message_hist AS
select
    case_id,
    Message_Subject,
    Is_Read,
    Create_Date,
    ext_field0,
    Message_ID,
    assign_to,
    object_id,
    ext_field5,
	record_eff_from_date,
	record_eff_to_date,
	active_record_ind,
	dml_ind
	from
	(select
        case_id,
        Message_Subject,
        Is_Read,
        Create_Date,
        ext_field0,
        Message_ID,
        assign_to,
        object_id,
        ext_field5,
		record_eff_from_date,
	    record_eff_to_date,
	    active_record_ind,
	    dml_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_clm_message_hist
		)
WHERE
	rnk = 1
	AND active_record_ind = 'Y';
	
create table #tb_t_clm_siu_file_hist AS
select
        investigation_no,
	    file_id,
	    record_eff_from_date,
	    record_eff_to_date,
	    active_record_ind,
	    dml_ind
	from
	(select
        investigation_no,
		file_id,
		record_eff_from_date,
	    record_eff_to_date,
	    active_record_ind,
	    dml_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_clm_siu_file_hist
		)
WHERE
	rnk = 1
	AND active_record_ind = 'Y';
	
create table #tb_t_clm_case_hist AS
select
    Claim_no,
	case_id,
	record_eff_from_date,
	record_eff_to_date,
	active_record_ind,
	dml_ind
	from
	(select
        Claim_no,
	    case_id,
	    record_eff_from_date,
	    record_eff_to_date,
	    active_record_ind,
	    dml_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_clm_case_hist
		)
WHERE
	rnk = 1
	AND active_record_ind = 'Y';
	
create table #tb_t_clm_object_hist AS
select
    Seq_no,
	Claim_Type_Desc,
	Object_id,
	record_eff_from_date,
	record_eff_to_date,
	active_record_ind,
	dml_ind
	from
	(select
        Seq_no,
	    Claim_Type_Desc,
	    Object_id,
	    record_eff_from_date,
	    record_eff_to_date,
	    active_record_ind,
	    dml_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_clm_object_hist
		)
WHERE
	rnk = 1
	AND active_record_ind = 'Y';
-------
-----------------------------SCRIPT LOGIC----------------------------
CREATE TABLE #tmpofficer AS     
	SELECT DISTINCT 
	--record_eff_from_date, 
	--dml_ind, 
  --active_record_ind,                  
  us.user_id 
 --case when rpt.SupervisorCode IS NULL then 'OTHERS' else rpt.Section end as Section                   
 , UPPER(us.User_Name) AS Officer_Code                    
 , UPPER(us.Real_Name) AS Officer_Name                    
 , UPPER(ussup.User_Name) AS Superior_Code                    
 , UPPER(ussup.Real_Name) AS Superior_Name      
 --, [Rank] = case when rpt.SupervisorCode IS NULL then (( select max(RankOrder) from GIV3.RptMTWklyClmUnreadMessageSupervisor ) + 1) else rpt.RankOrder end          
 FROM #tb_t_user_hist us     
 INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id                   
 INNER JOIN #tb_t_ptyr_hist er  ON  emp.pty_id = er.pty_id                    
 INNER JOIN #tb_t_ptyr_rela_hist re  ON er.ptyr_id = re.from_ptyr_id                  
 INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id                       
 INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id                      
 INNER JOIN #tb_t_pty_employee_hist e  ON er.ptyr_id = e.ptyr_id                    
 --INNER JOIN v_pty_dept_org dept ON us.dept_id = dept.dept_id     
 INNER JOIN #tb_t_user_hist ussup ON sup.pty_id = ussup.party_id   
 --LEFT OUTER JOIN GIV3.RptMTWklyClmUnreadMessageSupervisor rpt on rpt.SupervisorCode = ussup.User_Name and rpt.STATUS = 'A' -- added 19-01-2015                  
 WHERE re.relation_type = 57               
 and er.ptyr_type = 1                   
 and er.ptyr_status = 1                    
 and e.employee_status = 1               
 and re.status = 1         
 --and deptteam_code = 'MT'   
 and us.party_role='1'  
 and ussup.PARTY_ROLE='1'
;

----------------------
CREATE TABLE #PKPrimary_stg_driver AS 
SELECT user_id, record_eff_from_date, dml_ind, active_record_ind
FROM (

    SELECT us.user_id, us.record_eff_from_date, us.dml_ind, us.active_record_ind
    FROM #tb_t_user_hist us
   
    UNION

    SELECT us.user_id, emp.record_eff_from_date, emp.dml_ind, emp.active_record_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    
    UNION
	
	SELECT us.user_id, er.record_eff_from_date, er.dml_ind, er.active_record_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	
	UNION
	
	SELECT us.user_id, re.record_eff_from_date, re.dml_ind, re.active_record_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	
	UNION
	
	SELECT us.user_id, sr.record_eff_from_date, sr.dml_ind, sr.active_record_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	
	UNION
	
	SELECT us.user_id, sup.record_eff_from_date, sup.dml_ind, sup.active_record_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
	
	UNION
	
	SELECT us.user_id, e.record_eff_from_date, e.dml_ind, e.active_record_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
	INNER JOIN #tb_t_pty_employee_hist e ON er.ptyr_id = e.ptyr_id

	UNION
	
	SELECT us.user_id, ussup.record_eff_from_date, ussup.dml_ind, ussup.active_record_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
	INNER JOIN #tb_t_pty_employee_hist e ON er.ptyr_id = e.ptyr_id
	INNER JOIN #tb_t_user_hist ussup ON sup.pty_id = ussup.party_id
   );
	

CREATE TABLE #PKPrimary_driver AS
SELECT user_id, record_eff_from_date, dml_ind, active_record_ind
FROM (
	SELECT user_id, record_eff_from_date, dml_ind, active_record_ind,
	row_number() over( partition by user_id
	 order by 
	CASE WHEN dml_ind = 'D' THEN 1 else 2 END,record_eff_from_date desc ) rnk
	from #PKPrimary_stg_driver )
where rnk=1;


-------------------message ------------------------
CREATE TABLE #tmpmessage AS  
SELECT DISTINCT                   
 a.case_id AS Case_Id                    
 , a.Object_id AS Object_Id                               
 , CASE WHEN a.object_id IS NOT NULL THEN clm.Claim_no + '-' + obj.Seq_no ELSE fl.investigation_no END AS Claim_No	
 , a.Message_Subject AS Message_Subject                 
 , a.Is_Read AS Message_Read_Flag                    
 , a.Create_Date AS Message_Create_Date                    
 , CASE WHEN a.ext_field0 =  1 THEN 'LOW'                    
   WHEN a.ext_field0 =  2 THEN 'NORMAL'                     
   WHEN a.ext_field0 =  3 THEN 'HIGH' ELSE UPPER(a.ext_field0) END AS Message_Urgency                    
 , obj.Claim_Type_Desc                      
 --, t.Section                     
 , t.Officer_Code                    
 , t.Officer_Name                    
 , t.Superior_Code                    
 , t.Superior_Name               
 --, DATEDIFF(DD,a.Create_Date,@Rpttimestamp) AgingDays              
 --, t.Rank 
 ,T.user_id               
 ,a.Message_ID AS Message_ID     
 FROM #tb_t_clm_message_hist A    
 INNER JOIN #tmpofficer t ON a.assign_to = T.user_id          
 LEFT OUTER JOIN #tb_t_clm_case_hist clm ON a.case_id = clm.case_id                 
 LEFT OUTER JOIN #tb_t_clm_object_hist obj ON a.object_id = obj.object_id                                 
 LEFT OUTER JOIN #tb_t_clm_siu_file_hist fl ON A.ext_field5 = fl.file_id

-- where datediff(dd,a.Create_Date,@Rpttimestamp) > 0      
	;
	
--------------
CREATE TABLE #PKPrimary_stg_driver_message AS 
SELECT message_id, record_eff_from_date, dml_ind, active_record_ind
FROM (

    SELECT A.message_id, A.record_eff_from_date, A.dml_ind, A.active_record_ind
    FROM #tb_t_clm_message_hist A

  --  UNION

  --  SELECT A.message_id, t.record_eff_from_date, t.dml_ind, t.active_record_ind
  --  FROM #tb_t_clm_message_hist A
  --  INNER JOIN #tmpofficer t ON a.assign_to = T.user_id
    );
   

------------------PKPrimary message----------------------------
-- pk primary message
CREATE TABLE #PKPrimary_driver_message AS 
SELECT DISTINCT message_id, active_record_ind
FROM
(SELECT
		message_id,
		active_record_ind,
		ROW_NUMBER() OVER (
			PARTITION BY message_id
			ORDER BY CASE WHEN dml_ind = 'D' THEN 1 ELSE 2 END,
			record_eff_from_date DESC
			) rnk
	FROM 
		#PKPrimary_stg_driver_message)
WHERE 
	rnk = 1;

----------------count --------------------------------

CREATE TABLE #src_count AS
SELECT 'EBGI-tmpofficer' AS source_data_set, COUNT(*) AS source_count 
FROM (
	SELECT DISTINCT
		pk1.user_id
	FROM
		#PKPrimary_driver pk1
 INNER JOIN #tb_t_user_hist AS us ON pk1.user_id = us.user_id       
 INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id                   
 INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id                    
 INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id                  
 INNER JOIN #tb_t_ptyr_hist sr on re.to_ptyr_id = sr.ptyr_id                       
 INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id                      
 INNER JOIN #tb_t_pty_employee_hist e ON er.ptyr_id = e.ptyr_id                      
 INNER JOIN #tb_t_user_hist ussup ON sup.pty_id = ussup.party_id   
 WHERE re.relation_type = 57               
 and er.ptyr_type = 1                   
 and er.ptyr_status = 1                    
 and e.employee_status = 1 
 and re.status = 1 
 and us.party_role='1' 
 and ussup.PARTY_ROLE='1'
 )
UNION
SELECT 'EBGI-tmpmessage' AS source_data_set, COUNT(*) AS source_count 
FROM (
	SELECT DISTINCT
		pk2.message_id
	FROM
		#PKPrimary_driver_message pk2
 INNER JOIN #tb_t_clm_message_hist AS A ON pk2.message_id = A.message_id
 INNER JOIN #tmpofficer t ON a.assign_to = T.user_id          
 LEFT OUTER JOIN #tb_t_clm_case_hist clm ON a.case_id = clm.case_id 
 LEFT OUTER JOIN #tb_t_clm_object_hist obj ON a.object_id = obj.object_id                          
 LEFT OUTER JOIN #tb_t_clm_siu_file_hist fl ON A.ext_field5 = fl.file_id 
);


CREATE TABLE #tgt_count AS
SELECT COUNT(*) AS target_count, source_data_set
FROM cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message
WHERE active_record_ind = 'Y'
GROUP BY source_data_set; 

INSERT INTO cl_rpt_stg_temp.dq_audit
SELECT
	'RPT' AS app_name,
	'tl_ebgi_def' AS src_schema,
	'cl_rpt_stg' AS tgt_schema,
	'tb_rpt_gi_mt_wkly_clm_unread_message' AS tgt_table,
	source_data_set AS entity,
	'*' AS instance,
	'count' AS check_type,
	source_count AS src_records,
	target_count AS tgt_records,
	CASE WHEN target_count = source_count THEN 0 
		WHEN target_count != source_count THEN ABS(source_count - target_count)
		END AS diff,
	CASE WHEN target_count = source_count THEN 0
		WHEN target_count != source_count THEN ABS(
			(source_count - target_count) * 100 / (CASE WHEN COALESCE(source_count, 0) < 1 THEN 1 ELSE source_count END))
		END AS diff_percentage,
	CASE WHEN target_count = source_count THEN 'Success'
		WHEN ABS((source_count - target_count) * 100 / (CASE WHEN COALESCE(source_count, 0) < 1 THEN 1 ELSE source_count END)) <= 5 THEN 'Success'
		ELSE 'Failure'
		END AS status,
	GETDATE() AS report_date
FROM (
	SELECT s.source_data_set, s.source_count, t.target_count 
	FROM #src_count s 
	INNER JOIN #tgt_count t ON s.source_data_set = t.source_data_set
);
	
DROP TABLE IF EXISTS #tb_t_user_hist;
DROP TABLE IF EXISTS #tb_t_pty_indi_hist;
DROP TABLE IF EXISTS #tb_t_ptyr_hist;
DROP TABLE IF EXISTS #tb_t_ptyr_rela_hist;
DROP TABLE IF EXISTS #tb_t_pty_employee_hist;
DROP TABLE IF EXISTS #tb_t_clm_message_hist;
DROP TABLE IF EXISTS #tb_t_clm_case_hist;
DROP TABLE IF EXISTS #tb_t_clm_object_hist;
DROP TABLE IF EXISTS #tb_t_clm_siu_file_hist;
DROP TABLE IF EXISTS #tmpofficer;
DROP TABLE IF EXISTS #tmpmessage;
DROP TABLE IF EXISTS #PKPrimary_stg_driver;
DROP TABLE IF EXISTS #PKPrimary_driver;
DROP TABLE IF EXISTS #PKPrimary_driver_message;
DROP TABLE IF EXISTS #PKPrimary_stg_driver_message;
DROP TABLE IF EXISTS #src_count;
DROP TABLE IF EXISTS #tgt_count;

END;
