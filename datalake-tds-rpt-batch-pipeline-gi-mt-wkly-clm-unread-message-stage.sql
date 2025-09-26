BEGIN;

SET TIMEZONE = 'Singapore';

-- Set last run date for incremental load

CREATE TABLE #v_rundate_tmpofficer AS
SELECT 
	NVL(
		CAST(DATE_TRUNC('day', DATEADD(day, -1, src_record_eff_from_date)) AS TIMESTAMP), 
		CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)
	) AS v_vLastRunDate 
FROM (
	SELECT MAX(src_record_eff_from_date) AS src_record_eff_from_date
	FROM cl_rpt_stg_temp.ctrl_audit
	WHERE tgt_schema = 'cl_rpt_stg'
	AND tgt_table = 'tb_rpt_gi_mt_wkly_clm_unread_message'
	AND tgt_source_app_code = 'EBGI' AND tgt_source_data_set = 'EBGI'
	);
	
CREATE TABLE #v_rundate_tmpmessage AS
SELECT 
	NVL(
		CAST(DATE_TRUNC('day', DATEADD(day, -1, src_record_eff_from_date)) AS TIMESTAMP), 
		CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)
	) AS v_vLastRunDate 
FROM (
	SELECT MAX(src_record_eff_from_date) AS src_record_eff_from_date
	FROM cl_rpt_stg_temp.ctrl_audit
	WHERE tgt_schema = 'cl_rpt_stg'
	AND tgt_table = 'tb_rpt_gi_mt_wkly_clm_unread_message'
	AND tgt_source_app_code = 'EBGI' AND tgt_source_data_set = 'EBGI-tmpmessage'
	);
	
-- Create temp tables for source TDS driver tables with latest records
create table #tb_t_user_hist AS
select
    record_eff_from_date,
	dml_ind,
    user_id,
	party_id, 
    dept_id,
    party_role,
    user_name,
    real_name	
	from
	(select
        record_eff_from_date,
	    dml_ind,
        user_id,
	    party_id,
		dept_id,
		party_role,
		user_name,
        real_name,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_user_hist
		)
WHERE
	rnk = 1;
	
create table #tb_t_pty_indi_hist AS
select
    record_eff_from_date,
	dml_ind,
    pty_id	
	from
	(select
        record_eff_from_date,
	    dml_ind,
        pty_id,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_pty_indi_hist
		)
WHERE
	rnk = 1;
	
create table #tb_t_ptyr_hist AS
select
    record_eff_from_date,
	dml_ind,
    pty_id,
    ptyr_id,
    ptyr_type,
    ptyr_status	
	from
	(select
        record_eff_from_date,
	    dml_ind,
        pty_id,
		ptyr_id,
		ptyr_type,
        ptyr_status,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_ptyr_hist
		)
WHERE
	rnk = 1;

create table #tb_t_ptyr_rela_hist AS
select
    record_eff_from_date,
	dml_ind,
    from_ptyr_id,
    to_ptyr_id,
    relation_type,
    status	
	from
	(select
        record_eff_from_date,
	    dml_ind,
        from_ptyr_id,
		to_ptyr_id,
		relation_type,
		status,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_ptyr_rela_hist
		)
WHERE
	rnk = 1;

create table #tb_t_pty_employee_hist AS
select
    record_eff_from_date,
	dml_ind,
    ptyr_id,
	employee_status
	from
	(select
        record_eff_from_date,
	    dml_ind,
        ptyr_id,
		employee_status,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_pty_employee_hist
		)
WHERE
	rnk = 1;

create table #tb_t_clm_message_hist AS
select
    record_eff_from_date,
	dml_ind,
    case_id,
    Message_Subject,
    Is_Read,
    Create_Date,
    ext_field0,
    Message_ID,
    assign_to,
    object_id,
    ext_field5
	from
	(select
        record_eff_from_date,
	    dml_ind,
        case_id,
        Message_Subject,
        Is_Read,
        Create_Date,
        ext_field0,
        Message_ID,
        assign_to,
        object_id,
        ext_field5,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_clm_message_hist
		)
WHERE
	rnk = 1;
	
create table #tb_t_clm_siu_file_hist AS
select
    record_eff_from_date,
	  dml_ind,
    investigation_no,
	  file_id,
	  active_record_ind
	from
	(select
        record_eff_from_date,
	    dml_ind,
        investigation_no,
		file_id,
		active_record_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_clm_siu_file_hist
		)
WHERE
	rnk = 1;
	
create table #tb_t_clm_case_hist AS
select
    record_eff_from_date,
	dml_ind,
    Claim_no,
	case_id,
	active_record_ind
	from
	(select
        record_eff_from_date,
	    dml_ind,
        Claim_no,
	    case_id,
	    active_record_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_clm_case_hist
		)
WHERE
	rnk = 1;
	
create table #tb_t_clm_object_hist AS
select
    record_eff_from_date,
	dml_ind,
    Seq_no,
	Claim_Type_Desc,
	Object_id,
	active_record_ind
	from
	(select
        record_eff_from_date,
	    dml_ind,
        Seq_no,
	    Claim_Type_Desc,
	    Object_id,
	    active_record_ind,
		ROW_NUMBER() OVER (
			PARTITION BY business_key 
			ORDER BY COALESCE(CASE WHEN TRIM(change_seq)='' THEN '-1' ELSE change_seq END, '-1') DESC,
			record_eff_from_date DESC
			) rnk
	FROM
		tl_ebgi_def.tb_t_clm_object_hist
		)
WHERE
	rnk = 1;

create table #tb_v_pty_dept_org_hist AS
select
    record_eff_from_date,
	dml_ind,
    dept_id,
	active_record_ind
	FROM
		tl_ebgi_def.tb_v_pty_dept_org_hist
		);	
	
--Create target table with only this audit columns	
create table #tb_rpt_gi_mt_wkly_clm_unread_message AS
select
dml_ind,
checksum,
active_record_ind,
business_key,
source_app_code,
source_data_set
from(select
dml_ind,
checksum,
active_record_ind,
business_key,
source_app_code,
source_data_set,
row_number()over(partition by business_key order by record_eff_from_date desc,record_eff_to_date desc ) rnk
from cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message where source_app_code='EBGI')
where rnk=1;
	
--CREATE PKPRIMARY FOR ALL DRIVER TABLES
CREATE TABLE #PKPrimary_stg_driver AS 
SELECT user_id, record_eff_from_date, dml_ind
FROM (

    SELECT us.user_id, us.record_eff_from_date, us.dml_ind
    FROM #tb_t_user_hist us
   
    UNION

    SELECT us.user_id, emp.record_eff_from_date, emp.dml_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    
    UNION
	
	SELECT us.user_id, er.record_eff_from_date, er.dml_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	
	UNION
	
	SELECT us.user_id, re.record_eff_from_date, re.dml_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	
	UNION
	
	SELECT us.user_id, sr.record_eff_from_date, sr.dml_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	
	UNION
	
	SELECT us.user_id, sup.record_eff_from_date, sup.dml_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
	
	UNION
	
	SELECT us.user_id, e.record_eff_from_date, e.dml_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
	INNER JOIN #tb_t_pty_employee_hist e ON er.ptyr_id = e.ptyr_id

    UNION

    SELECT us.user_id, dept.record_eff_from_date, dept.dml_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
	INNER JOIN #tb_t_pty_employee_hist e ON er.ptyr_id = e.ptyr_id
	INNER JOIN #tb_v_pty_dept_org_hist dept ON us.dept_id = dept.dept_id
	
	UNION
	
	SELECT us.user_id, ussup.record_eff_from_date, ussup.dml_ind
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
	INNER JOIN #tb_t_pty_employee_hist e ON er.ptyr_id = e.ptyr_id
	INNER JOIN #tb_v_pty_dept_org_hist dept ON us.dept_id = dept.dept_id
	INNER JOIN #tb_t_user_hist ussup ON sup.pty_id = ussup.party_id
   );
   
	
--PKPRIMARY TO GET DML_IND
CREATE TABLE #PKPrimary_driver AS
SELECT user_id, dml_ind
FROM (
    SELECT
        user_id,
        dml_ind,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY CASE WHEN dml_ind = 'D' THEN 1 ELSE 2 END, record_eff_from_date DESC
        ) rnk
    FROM #PKPrimary_stg_driver
)
WHERE rnk = 1;

--CREATE PKPRIMARY STG FOR ALL TDS AND DRIVER TABLES
CREATE TABLE #PKPrimary_stg AS
SELECT user_id, record_eff_from_date
FROM (

    SELECT us.user_id, us.record_eff_from_date
    FROM #tb_t_user_hist us
    WHERE us.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpofficer)

    UNION

    SELECT us.user_id, emp.record_eff_from_date
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
    WHERE emp.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpofficer)

    UNION
	
	SELECT us.user_id, er.record_eff_from_date
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
	INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
    WHERE er.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpofficer)
	
	UNION
	
	SELECT us.user_id, re.record_eff_from_date
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
	INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
    WHERE re.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpofficer)
	
	UNION
	
	SELECT us.user_id, sr.record_eff_from_date
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
	INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
    WHERE sr.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpofficer)
	
	UNION
	
	SELECT us.user_id, sup.record_eff_from_date
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
	INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
    WHERE sup.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpofficer)
	
	UNION
	
	SELECT us.user_id, e.record_eff_from_date
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
	INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
	INNER JOIN #tb_t_pty_employee_hist e  ON er.ptyr_id = e.ptyr_id
    WHERE e.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpofficer)
    
	UNION
	
	SELECT us.user_id, dept.record_eff_from_date
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
	INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
	INNER JOIN #tb_t_pty_employee_hist e  ON er.ptyr_id = e.ptyr_id
	INNER JOIN #tb_v_pty_dept_org_hist dept ON us.dept_id = dept.dept_id
	WHERE dept.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpofficer)
    
    UNION
	
	SELECT us.user_id, ussup.record_eff_from_date
    FROM #tb_t_user_hist us
    INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id
	INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id
	INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id
	INNER JOIN #tb_t_ptyr_hist sr on  re.to_ptyr_id = sr.ptyr_id
	INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id
	INNER JOIN #tb_t_pty_employee_hist e  ON er.ptyr_id = e.ptyr_id
	INNER JOIN #tb_v_pty_dept_org_hist dept ON us.dept_id = dept.dept_id
	INNER JOIN #tb_t_user_hist ussup ON sup.pty_id = ussup.party_id
    WHERE ussup.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpofficer)
	
);


CREATE TABLE #PKPrimary_total AS
SELECT user_id, record_eff_from_date
FROM (
    SELECT
        user_id,
        record_eff_from_date,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY record_eff_from_date DESC
        ) rnk
    FROM #PKPrimary_stg
)
WHERE rnk = 1;

CREATE TABLE #PKPrimary AS
SELECT a.user_id, a.record_eff_from_date, CASE WHEN b.dml_ind <> 'D' THEN 'I' ELSE b.dml_ind END AS dml_ind
FROM #PKPrimary_total a 
INNER JOIN #PKPrimary_driver b ON a.user_id = b.user_id ;

CREATE TABLE #tmpofficer AS

SELECT DISTINCT 
      'EBGI' AS Source_app_code,
      'EBGI' AS source_data_set,
       pk.dml_ind,
       pk.record_eff_from_date,   
       SHA2('EBGI~' || us.user_id , 256) as uuid,
       ('EBGI~' || us.user_id ) as business_key,
	   us.user_id
 --case when rpt.SupervisorCode IS NULL then 'OTHERS' else rpt.Section end as Section
 , UPPER(us.User_Name) AS Officer_Code                    
 , UPPER(us.Real_Name) AS Officer_Name                    
 , UPPER(ussup.User_Name) AS Superior_Code                    
 , UPPER(ussup.Real_Name) AS Superior_Name      
 --, [Rank] = case when rpt.SupervisorCode IS NULL then (( select max(RankOrder) from GIV3.RptMTWklyClmUnreadMessageSupervisor ) + 1) else rpt.RankOrder end 
 FROM #pkprimary pk inner join #tb_t_user_hist AS us ON pk.user_id = us.user_id       
 INNER JOIN #tb_t_pty_indi_hist emp on emp.pty_id = us.party_id                   
 INNER JOIN #tb_t_ptyr_hist er ON emp.pty_id = er.pty_id                    
 INNER JOIN #tb_t_ptyr_rela_hist re ON er.ptyr_id = re.from_ptyr_id                  
 INNER JOIN #tb_t_ptyr_hist sr on re.to_ptyr_id = sr.ptyr_id                       
 INNER JOIN #tb_t_pty_indi_hist sup on sr.pty_id = sup.pty_id                      
 INNER JOIN #tb_t_pty_employee_hist e ON er.ptyr_id = e.ptyr_id 
 INNER JOIN #tb_v_pty_dept_org_hist dept ON us.dept_id = dept.dept_id  
 INNER JOIN #tb_t_user_hist ussup ON sup.pty_id = ussup.party_id   
 --LEFT OUTER JOIN GIV3.RptMTWklyClmUnreadMessageSupervisor rpt on rpt.SupervisorCode = ussup.User_Name and rpt.STATUS = 'A'            
 WHERE re.relation_type = 57               
 and er.ptyr_type = 1                   
 and er.ptyr_status = 1                    
 and e.employee_status = 1 
 and re.status = 1        
 and us.dept_code = 'MT'   
 and us.party_role='1' 
 and ussup.PARTY_ROLE='1'
;


--TEMP tempmessage

--CREATE PKPRIMARY FOR ALL DRIVER TABLES
CREATE TABLE #PKPrimary_stg_driver_message AS 
SELECT message_id, record_eff_from_date, dml_ind
FROM (

    SELECT A.message_id, A.record_eff_from_date, A.dml_ind
    FROM #tb_t_clm_message_hist A

    UNION

    SELECT A.message_id, t.record_eff_from_date, t.dml_ind
    FROM #tb_t_clm_message_hist A
    INNER JOIN #tmpofficer t ON a.assign_to = T.user_id
    );
   
	
--PKPRIMARY TO GET DML_IND
CREATE TABLE #PKPrimary_driver_message AS
SELECT message_id, dml_ind
FROM (
    SELECT
	    message_id,
        dml_ind,
        ROW_NUMBER() OVER (
            PARTITION BY message_id
            ORDER BY CASE WHEN dml_ind = 'D' THEN 1 ELSE 2 END, record_eff_from_date DESC
        ) rnk
    FROM #PKPrimary_stg_driver_message
)
WHERE rnk = 1;

--CREATE PKPRIMARY STG FOR ALL TDS AND DRIVER TABLES
CREATE TABLE #PKPrimary_stg_message AS
SELECT message_id, record_eff_from_date
FROM (

    SELECT A.message_id, A.record_eff_from_date
    FROM #tb_t_clm_message_hist A
    WHERE A.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpmessage)

    UNION

    SELECT A.message_id, t.record_eff_from_date
    FROM #tb_t_clm_message_hist A
	INNER JOIN #tmpofficer t ON a.assign_to = T.user_id
    WHERE t.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpmessage)

    UNION
	
	SELECT A.message_id, clm.record_eff_from_date
    FROM #tb_t_clm_message_hist A
	INNER JOIN #tmpofficer t ON a.assign_to = T.user_id
    INNER JOIN #tb_t_clm_case_hist clm ON a.case_id = clm.case_id
    WHERE clm.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpmessage)
	
	UNION
	
	SELECT A.message_id, obj.record_eff_from_date
    FROM #tb_t_clm_message_hist A
	INNER JOIN #tmpofficer t ON a.assign_to = T.user_id
    INNER JOIN #tb_t_clm_case_hist clm ON a.case_id = clm.case_id
	INNER JOIN #tb_t_clm_object_hist obj ON a.object_id = obj.object_id
    WHERE obj.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpmessage)

	UNION
	
	SELECT A.message_id, fl.record_eff_from_date
    FROM #tb_t_clm_message_hist A
	INNER JOIN #tmpofficer t ON a.assign_to = T.user_id
    INNER JOIN #tb_t_clm_case_hist clm ON a.case_id = clm.case_id
	INNER JOIN #tb_t_clm_object_hist obj ON a.object_id = obj.object_id
	INNER JOIN #tb_t_clm_siu_file_hist fl ON A.ext_field5 = fl.file_id
    WHERE fl.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate_tmpmessage)
	
	);
	
		
CREATE TABLE #PKPrimary_total_message AS
SELECT message_id, record_eff_from_date
FROM (
    SELECT
	    message_id,
        record_eff_from_date,
        ROW_NUMBER() OVER (
            PARTITION BY message_id
            ORDER BY record_eff_from_date DESC
        ) rnk
    FROM #PKPrimary_stg_message
)
WHERE rnk = 1;

CREATE TABLE #PKPrimary_message AS
SELECT a.message_id, a.record_eff_from_date, CASE WHEN b.dml_ind <> 'D' THEN 'I' ELSE b.dml_ind END AS dml_ind
FROM #PKPrimary_total_message a 
INNER JOIN #PKPrimary_driver_message b ON a.message_id = b.message_id
;

CREATE TABLE #tmpmessage AS
SELECT	DISTINCT 
        'EBGI' AS Source_app_code,
        'EBGI-tmpmessage' AS source_data_set,
        pk.dml_ind,
        pk.record_eff_from_date,   
        SHA2('EBGI~' || a.Message_ID, 256) as uuid, 
       ('EBGI~' || a.Message_ID ) as business_key,  
        t.user_id,               
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
        , t.Officer_Code                    
        , t.Officer_Name                    
        , t.Superior_Code                    
        , t.Superior_Name               
        --, DATEDIFF(DD, a.Create_Date, @Rpttimestamp) Aging_Days                 
        , a.Message_ID AS Message_ID   
 FROM #pkprimary_message pk inner join #tb_t_clm_message_hist AS A ON pk.message_id = A.message_id
 INNER JOIN #tmpofficer t ON a.assign_to = T.user_id          
 LEFT OUTER JOIN #tb_t_clm_case_hist clm ON a.case_id = clm.case_id AND clm.active_record_ind='Y'
 LEFT OUTER JOIN #tb_t_clm_object_hist obj ON a.object_id = obj.object_id AND obj.active_record_ind='Y'                          LEFT OUTER JOIN #tb_t_clm_siu_file_hist fl ON A.ext_field5 = fl.file_id AND fl.active_record_ind='Y'
;
--where datediff(dd,a.Create_Date,@Rpttimestamp) > 0 ;

SELECT case_id,object_id,claim_no,message_subject,message_read_flag,message_create_date,message_urgency,claim_type_desc,officer_code,officer_name,superior_code,superior_name,Message_ID    
FROM #tmpmessage    
UNION ALL    
SELECT null,null,null,null,null,null,null,null,officer_code,officer_name,superior_code,superior_name,Null     
from #tmpofficer 
where officer_name not in(select distinct officer_name from #tmpmessage) ;
 
--update dml_ind for slave incremental scd type-2
UPDATE #tmpmessage
		SET dml_ind= case when a.dml_ind<>'D' then 'U' else a.dml_ind end
		FROM #tmpmessage a, #tb_rpt_gi_mt_wkly_clm_unread_message b
		WHERE b.active_record_ind='Y'
		AND a.business_key = b.business_key;

-- Table with checksum
CREATE	TABLE #hashstgtmpmessage as
	SELECT
		*,
        SHA2(	
    coalesce(cast(source_app_code as varchar), cast('null' as varchar))+
	coalesce(cast(source_data_set as varchar), cast('null' as varchar))+
    coalesce(cast(uuid as varchar), cast('null' as varchar))+
	coalesce(cast(business_key as varchar), cast('null' as varchar))+
    coalesce(cast(case_id as varchar), cast('null' as varchar))+
	coalesce(cast(object_id as varchar), cast('null' as varchar))+
	coalesce(cast(claim_no as varchar), cast('null' as varchar))+
    coalesce(cast(message_subject as varchar), cast('null' as varchar))+
    coalesce(cast(message_read_flag as varchar), cast('null' as varchar))+
    coalesce(cast(message_create_date as varchar), cast('null' as varchar))+
    coalesce(cast(message_urgency as varchar), cast('null' as varchar))+
    coalesce(cast(claim_type_desc as varchar), cast('null' as varchar))+
    coalesce(cast(officer_code as varchar), cast('null' as varchar))+
	coalesce(cast(officer_name as varchar), cast('null' as varchar))+
	coalesce(cast(superior_code as varchar), cast('null' as varchar))+
	coalesce(cast(superior_name as varchar), cast('null' as varchar))+
	coalesce(cast(message_id as varchar), cast('null' as varchar))
, 256) 
AS checksum
FROM
	#tmpmessage
;		

-- Final tmpmessage table to get distinct records
create table #stgtmpmessage AS
select * from (select a.* , case when a.dml_ind='D' and b.dml_ind<>'D' then 1 when a.dml_ind in('I','U') AND b.dml_ind='D' THEN 1 when a.checksum <> coalesce(b.checksum,'1') and coalesce(b.active_record_ind,'Y')='Y' then 1 else 0 end as changed_rec_check from #hashstgtmpmessage a left outer join #tb_rpt_gi_mt_wkly_clm_unread_message b on a.business_key = b.business_key ) where changed_rec_check =1;

-- Truncate and insert into staging table
DELETE FROM cl_rpt_stg_temp.stg_tb_rpt_gi_mt_wkly_clm_unread_message WHERE 1=1;


INSERT INTO cl_rpt_stg_temp.stg_tb_rpt_gi_mt_wkly_clm_unread_message(
	source_app_code,
	source_data_set,
	dml_ind,
	record_created_date,
	record_updated_date,
	record_created_by,
	record_updated_by,
	record_eff_from_date,
	record_eff_to_date,
	active_record_ind,
	checksum,
	uuid,
	business_key,
	case_id 
	,object_id 
	,claim_no 
	,message_subject  
	,message_read_flag 
	,message_create_date 
	,message_urgency  
	,claim_type_desc 
	,officer_code  
	,officer_name 
	,superior_code  
	,superior_name  
	,message_id 
)
SELECT DISTINCT
	source_app_code,
	source_data_set,
	dml_ind,
	GETDATE() AS record_created_date,
	GETDATE() AS record_updated_date,
	'tds_etl' AS record_created_by,
	'tds_etl' AS record_updated_by,
	record_eff_from_date,
	CAST('9999-12-31 00:00:00.000000' AS timestamp) AS record_eff_to_date,
	'Y' AS active_record_ind,
	checksum,
	uuid,
	business_key,
	case_id 
	,object_id 
	,claim_no 
	,message_subject  
	,message_read_flag 
	,message_create_date 
	,message_urgency  
	,claim_type_desc 
	,officer_code  
	,officer_name 
	,superior_code  
	,superior_name 
	,message_id 
From #stgtmpmessage; 

DROP TABLE IF EXISTS #tb_t_user_hist;
DROP TABLE IF EXISTS #tb_t_pty_indi_hist;
DROP TABLE IF EXISTS #tb_t_ptyr_hist;
DROP TABLE IF EXISTS #tb_t_ptyr_rela_hist;
DROP TABLE IF EXISTS #tb_t_pty_employee_hist;
DROP TABLE IF EXISTS #tb_t_clm_message_hist;
DROP TABLE IF EXISTS #tb_v_pty_dept_org_hist;
DROP TABLE IF EXISTS #tmpofficer;
DROP TABLE IF EXISTS #tb_t_clm_case_hist;
DROP TABLE IF EXISTS #tb_t_clm_object_hist;
DROP TABLE IF EXISTS #tb_t_clm_siu_file_hist;
DROP TABLE IF EXISTS #PKPrimary_stg_driver;
DROP TABLE IF EXISTS #PKPrimary_driver;
DROP TABLE IF EXISTS #PKPrimary_stg;
DROP TABLE IF EXISTS #PKPrimary_total;
DROP TABLE IF EXISTS #PKPrimary;
DROP TABLE IF EXISTS #tmpmessage;
DROP TABLE IF EXISTS #PKPrimary_stg_driver_message;
DROP TABLE IF EXISTS #PKPrimary_driver_message;
DROP TABLE IF EXISTS #PKPrimary_stg_message;
DROP TABLE IF EXISTS #PKPrimary_total_message;
DROP TABLE IF EXISTS #PKPrimary_message;
DROP TABLE IF EXISTS #hashstgtmpmessage;
DROP TABLE IF EXISTS #stgtmpmessage;
DROP TABLE IF EXISTS #v_rundate_tmpofficer;
DROP TABLE IF EXISTS #v_rundate_tmpmessage;

END;
