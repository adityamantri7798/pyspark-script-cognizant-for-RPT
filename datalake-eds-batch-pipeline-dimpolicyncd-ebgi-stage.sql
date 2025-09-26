BEGIN;

SET TIMEZONE = 'Singapore';

CREATE TABLE #v_rundate AS
SELECT 
    nvl(CAST(DATE_TRUNC('day',DATEADD (day,-1,src_record_eff_from_date)) AS TIMESTAMP),CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS v_vLastRunDate
FROM (
    SELECT MAX(src_record_eff_from_date) AS src_record_eff_from_date
    FROM el_eds_def_stg.ctrl_audit 
    WHERE
        tgt_table_name='dimpolicyncd' 
        AND tgt_source_app_code='EBGI'
);

--creating temp tables for both eds and tds tables

create table #tb_t_ncd_relation_hist as 
select REF_ID
	,NCD_REF_ID
	,NCD_LEVEL
	,PRODUCT_CODE
	,status
	,NCD_ID
	,operate_type
	,record_eff_from_date
	,dml_ind
from(select
	REF_ID
	,NCD_REF_ID
	,NCD_LEVEL
	,PRODUCT_CODE
	,status
	,NCD_ID
	,operate_type
	,record_eff_from_date
	,dml_ind
	,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc,record_eff_from_date desc, record_eff_to_date desc) rnk
from tl_ebgi_def.tb_t_ncd_relation_hist)
where rnk = 1;

create table #tb_t_ncd_level_define_hist as 
select 
     NCD_VALUE
	,NCD_LEVEL
	,PRODUCT_CODE 
	,record_eff_from_date
	,active_record_ind
from(select 
    NCD_VALUE
	,NCD_LEVEL
	,PRODUCT_CODE 
	,record_eff_from_date
	,active_record_ind
	,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc,record_eff_from_date desc, record_eff_to_date desc) rnk
    from tl_ebgi_def.tb_t_ncd_level_define_hist) 
where rnk=1;
 
create table #tb_t_ncd_hist as 
select NCD_ID
	,OUT_COMPANY_ID
	,in_company_id
	,IN_EXPIRE_DATE
	,IN_REG_NO
	,IN_POLICY_NO
	,VERIFY_STATUS
	,STATUS
	,NCD_LEVEL
	,verify_status_desc
	,status_desc
	,in_company_name
	,out_company_name
	,record_eff_from_date
	,dml_ind
from (select
		NCD_ID
		,OUT_COMPANY_ID
		,in_company_id
		,IN_EXPIRE_DATE
		,IN_REG_NO
		,IN_POLICY_NO
		,VERIFY_STATUS
		,STATUS
		,NCD_LEVEL
		,verify_status_desc
		,status_desc
		,in_company_name
		,out_company_name
		,record_eff_from_date
		,dml_ind
		,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc,record_eff_from_date desc, record_eff_to_date desc) rnk
from tl_ebgi_def.tb_t_ncd_hist where source_app_code = 'EBGI') 
where rnk = 1;

  
create table #tb_t_ncd_protection_hist as 
select POLICY_ID
	,STATUS
	,WAIVE_CHARGE_FLAG
	,p_id
	,record_eff_from_date
	,active_record_ind
	,dml_ind
from(select 
    POLICY_ID
    ,STATUS
	,WAIVE_CHARGE_FLAG
	,p_id
	,record_eff_from_date
	,active_record_ind
	,dml_ind
	,row_number() over(partition by business_key order by coalesce(change_seq,-1) desc,record_eff_from_date desc, record_eff_to_date desc) rnk
from tl_ebgi_def.tb_t_ncd_protection_hist
where source_app_code = 'EBGI')
where rnk=1;

create table #dimpolicy as 
select policy_id
	,policy_uuid
	,policy_no
	,source_app_code
from(
	select policy_id
		,policy_uuid
		,policy_no
		,source_app_code
		,row_number() over(partition by business_key order by record_eff_from_date desc) rnk
	from el_eds_def.dimpolicy where source_app_code = 'EBGI')
where rnk = 1;

CREATE TABLE #PKPrimary_stg_driver AS 
SELECT ref_id, record_eff_from_date, dml_ind
FROM
(
		SELECT 
			a.ref_id, a.record_eff_from_date, a.dml_ind
		FROM 
		#tb_t_ncd_relation_hist a  
	UNION 
		Select 
			a.ref_id, b.record_eff_from_date, b.dml_ind
		FROM 
		#tb_t_ncd_relation_hist a
		inner join #tb_t_ncd_hist b on a.ncd_id = b.ncd_id
);

CREATE TABLE #PKPrimary_driver AS
Select ref_id, dml_ind
FROM(Select
ref_id,
dml_ind,
row_number() over( partition by ref_id order by CASE WHEN dml_ind = 'D' THEN 1 else 2 END,record_eff_from_date desc ) rnk
from #PKPrimary_stg_driver )
where rnk=1;

CREATE TABLE #PKPrimary_stg AS 
SELECT ref_id, record_eff_from_date
FROM
(
		SELECT 
			a.ref_id, a.record_eff_from_date
		FROM 
		#tb_t_ncd_relation_hist a 
		WHERE a.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate) 
	UNION 
		Select 
			a.ref_id, b.record_eff_from_date
		FROM 
		#tb_t_ncd_relation_hist a
		inner join #tb_t_ncd_hist b on a.ncd_id = b.ncd_id
		WHERE b.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate) 
	UNION 
	    SELECT 
	    	a.ref_id, ptc.record_eff_from_date
	    FROM 
	    #tb_t_ncd_protection_hist ptc
		inner join #tb_t_ncd_relation_hist a on a.ref_id = ptc.policy_id
	    WHERE ptc.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate)
	UNION
		SELECT A.ref_id, LV.record_eff_from_date
		FROM #tb_t_ncd_relation_hist A inner join #tb_t_ncd_level_define_hist LV on LV.NCD_LEVEL = A.NCD_LEVEL AND LV.PRODUCT_CODE = A.PRODUCT_CODE
		WHERE LV.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate)
	UNION
		SELECT A.ref_id, LV.record_eff_from_date
		FROM #tb_t_ncd_relation_hist A
		inner join #tb_t_ncd_hist B on A.ncd_id = B.ncd_id inner join #tb_t_ncd_level_define_hist LV on LV.NCD_LEVEL = B.NCD_LEVEL AND LV.PRODUCT_CODE = A.PRODUCT_CODE
		WHERE LV.record_eff_from_date >= (SELECT v_vLastRunDate FROM #v_rundate)
);

CREATE TABLE #PKPrimary_total AS
Select ref_id, record_eff_from_date
FROM(Select
ref_id,
record_eff_from_date,
row_number() over( partition by ref_id order by record_eff_from_date desc ) rnk
from #PKPrimary_stg )
where rnk=1;

CREATE TABLE #PKPrimary AS
Select a.ref_id, a.record_eff_from_date, case when b.dml_ind<>'D' then 'I' else b.dml_ind end as dml_ind
from #PKPrimary_total a inner join #PKPrimary_driver b on
a.ref_id=b.ref_id;

create table #dimpolicyncd AS
select
dml_ind,
checksum,
active_record_ind,
business_key
from(select
dml_ind,
checksum,
active_record_ind,
business_key,
row_number()over(partition by business_key order by  record_eff_from_date desc,record_eff_to_date desc ) rnk
from el_eds_def.dimpolicyncd where source_app_code='EBGI')
where rnk=1;

CREATE TABLE #tempstgDimGIPolicyNCD 
(
	source_app_code VARCHAR(100)   
	,source_data_set VARCHAR(100)   
	,dml_ind VARCHAR(100)   
	,record_created_date TIMESTAMP 
	,record_updated_date TIMESTAMP 
	,record_created_by VARCHAR(100)   
	,record_updated_by VARCHAR(100)   
	,record_eff_from_date TIMESTAMP 
	,record_eff_to_date TIMESTAMP 
	,active_record_ind VARCHAR(100)   
	,policy_ncd_uuid VARCHAR(1000)   
	,business_key VARCHAR(1000)   
	,policy_uuid VARCHAR(1000)   
	,policy_id INTEGER   
	,policy_no VARCHAR(50)   
	,product_code VARCHAR(50)   
	,ncd_policy_id INTEGER   
	,ncd_id INTEGER   
	,policy_ncd_level INTEGER   
	,policy_ncd_value NUMERIC(5,4)   
	,policy_ncd_status INTEGER   
	,policy_ncd_status_desc VARCHAR(20)   
	,ncd_level INTEGER   
	,ncd_value NUMERIC(5,4)   
	,ncd_status INTEGER   
	,ncd_status_desc VARCHAR(20)   
	,ncd_verify_status INTEGER   
	,ncd_in_policy_no VARCHAR(80)   
	,ncd_in_reg_no VARCHAR(100)   
	,ncd_in_expiry_date DATE   
	,ncd_verify_status_desc VARCHAR(20)   
	,ncd_in_company VARCHAR(100)   
	,ncd_out_company VARCHAR(100)   
	,ncd_protection VARCHAR(1)   
	,ncd_protection_waiver VARCHAR(1)   
);

insert into #tempstgDimGIPolicyNCD	
(
	source_app_code
	,source_data_set 
	,dml_ind
	,record_created_date
	,record_updated_date
	,record_created_by 
	,record_updated_by 
	,record_eff_from_date
	,record_eff_to_date
	,active_record_ind
	,policy_ncd_uuid
	,business_key
	,policy_uuid
	,policy_id
	,policy_no
	,product_code
	,ncd_policy_id
	,ncd_id
	,policy_ncd_level
	,policy_ncd_value
	,policy_ncd_status
	,policy_ncd_status_desc
	,ncd_level
	,ncd_value
	,ncd_status
	,ncd_status_desc
	,ncd_verify_status
	,ncd_in_policy_no
	,ncd_in_reg_no
	,ncd_in_expiry_date
	,ncd_verify_status_desc
	,ncd_in_company
	,ncd_out_company
	,ncd_protection
	,ncd_protection_waiver
	)	
select  
	'EBGI' as source_app_code,
	'EBGI' as source_data_set, 
	pk.dml_ind,
	getdate() as record_created_date,
	getdate() as record_updated_date,
	'EDS' as record_created_by ,
	'EDS' as record_updated_by ,
	pk.record_eff_from_date,
	cast('9999-12-31 00:00:00.000000' as timestamp) as record_eff_to_date,
	'Y' as active_record_ind,
	sha2('EBGI' || '~' || cast(a.ref_id as varchar),256) as policy_ncd_uuid,
	('EBGI' || '~' || cast(a.ref_id as varchar)) as business_key,
	pol.policy_uuid as policy_uuid,
	a.ref_id as policy_id,
	pol.policy_no as policy_no,
	a.product_code as product_code,
	a.ncd_ref_id as ncd_policy_id,
	a.ncd_id as ncd_id,
	a.ncd_level as policy_ncd_level,
	(select NCD_value from #tb_t_ncd_level_define_hist 
	where ncd_level = a.ncd_level and product_code = a.product_code
	AND active_record_ind='Y') as policy_ncd_value,
	a.status as policy_ncd_status,
	case when a.status = 1 then 'LOCK' else 'UNLOCK' end as policy_ncd_status_desc,
	b.ncd_level as ncd_level,
	(select NCD_value from #tb_t_ncd_level_define_hist 
	where ncd_level = b.ncd_level 
	and product_code = a.product_code AND active_record_ind='Y') as ncd_value,
	b.status as ncd_status,
	b.status_desc as ncd_status_desc,
	b.verify_status as ncd_verify_status,
	b.in_policy_no as ncd_in_policy_no,
	b.in_reg_no as ncd_in_reg_no,
	b.in_expire_date as ncd_in_expiry_date,
	case 	
		when ISNULL(cast(b.verify_status as varchar), '') = '' then 'NOT REPLIED'
		when ISNULL(cast(b.verify_status as varchar), '') in ('0', '1') then b.verify_status_desc else 'NOT REPLIED' end as ncd_verify_status_desc,
	b.in_company_name as ncd_in_company,
	b.out_company_name as ncd_out_company,
	case when ISNULL(ptc.status,0) = 1 then 'Y'
		else 'N'
	end as ncd_protection,
	case when ISNULL(ptc.waive_charge_flag,0) = 1 then 'Y'
		else 'N'
	end as ncd_protection_waiver		
	--from #tb_t_ncd_relation_hist a
FROM #PKPrimary pk
inner join #tb_t_ncd_relation_hist a on pk.ref_id = a.ref_id
inner join #tb_t_ncd_hist b on a.ncd_id = b.ncd_id
inner join #dimpolicy pol on a.ref_id = pol.policy_id and pol.source_app_code = 'EBGI'
left join 
(
		select 
				row_number () over (partition by policy_id order by p_id desc) rn,
				policy_id,
				status,
				waive_charge_flag,
				record_eff_from_date
		from #tb_t_ncd_protection_hist
		WHERE active_record_ind='Y'
) ptc on a.ref_id = ptc.policy_id and ptc.rn = 1
where a.ncd_level <> 0
and 
(
	(
		a.ncd_ref_id in 
		(
			select max(t.ncd_ref_id) from #tb_t_ncd_relation_hist t 
			where t.ref_id = a.ref_id and t.ncd_level <> 0
			and not exists 
			(
				select 1 from #tb_t_ncd_relation_hist 
				where ref_id = t.ref_id and ncd_level <> 0 and (status = 1 or  status = 9 and operate_type in (131, 102, 101, 122, 127))
			)
		)
	)
		or		  
	(
	a.ncd_ref_id in 
		(
			select max(x.ncd_ref_id) from #tb_t_ncd_relation_hist x 
			where x.ref_id = a.ref_id and (x.status = 1 or x.status = 9 and x.operate_type in (131, 102, 101, 122, 127))
		)
	)
);

UPDATE #tempstgDimGIPolicyNCD
		SET dml_ind= case when a.dml_ind<>'D' then 'U' else a.dml_ind end
		FROM #tempstgDimGIPolicyNCD a INNER JOIN #dimpolicyncd b ON a.business_key = b.business_key
		WHERE  b.active_record_ind='Y';
	  
create table #hashStgDimGiPolicyncd as
SELECT 
	source_app_code
	,source_data_set 
	,dml_ind
	,record_created_date
	,record_updated_date
	,record_created_by 
	,record_updated_by 
	,record_eff_from_date
	,record_eff_to_date
	,active_record_ind
	,policy_ncd_uuid
	,business_key
	,policy_uuid
	,policy_id
	,policy_no
	,product_code
	,ncd_policy_id
	,ncd_id
	,policy_ncd_level
	,policy_ncd_value
	,policy_ncd_status
	,policy_ncd_status_desc
	,ncd_level
	,ncd_value
	,ncd_status
	,ncd_status_desc
	,ncd_verify_status
	,ncd_in_policy_no
	,ncd_in_reg_no
	,ncd_in_expiry_date
	,ncd_verify_status_desc
	,ncd_in_company
	,ncd_out_company
	,ncd_protection
	,ncd_protection_waiver
	,sha2(
coalesce(cast(policy_ncd_uuid as varchar),cast('null' as varchar))+
coalesce(cast(business_key as varchar),cast('null' as varchar))+
coalesce(cast(policy_uuid	as varchar),cast('null' as varchar))+
coalesce(cast(policy_id	as varchar),cast('null' as varchar))+
coalesce(cast(policy_no	as varchar),cast('null' as varchar))+
coalesce(cast(product_code as varchar),cast('null' as varchar))+
coalesce(cast(ncd_policy_id as varchar),cast('null' as varchar))+
coalesce(cast(ncd_id as varchar),cast('null' as varchar))+
coalesce(cast(policy_ncd_level as varchar),cast('null' as varchar))+
coalesce(cast(policy_ncd_value as varchar),cast('null' as varchar))+
coalesce(cast(policy_ncd_status as varchar),cast('null' as varchar))+
coalesce(cast(policy_ncd_status_desc	 as varchar),cast('null' as varchar))+
coalesce(cast(ncd_level	as varchar),cast('null' as varchar))+
coalesce(cast(ncd_value	as varchar),cast('null' as varchar))+
coalesce(cast(ncd_status as varchar),cast('null' as varchar))+
coalesce(cast(ncd_status_desc as varchar),cast('null' as varchar))+
coalesce(cast(ncd_verify_status	as varchar),cast('null' as varchar))+
coalesce(cast(ncd_in_policy_no as varchar),cast('null' as varchar))+
coalesce(cast(ncd_in_reg_no	as varchar),cast('null' as varchar))+
coalesce(cast(ncd_in_expiry_date as varchar),cast('null' as varchar))+
coalesce(cast(ncd_verify_status_desc as varchar),cast('null' as varchar))+
coalesce(cast(ncd_in_company as varchar),cast('null' as varchar))+
coalesce(cast(ncd_out_company as varchar),cast('null' as varchar))+
coalesce(cast(ncd_protection as varchar),cast('null' as varchar))+
coalesce(cast(ncd_protection_waiver	 as varchar),cast('null' as varchar))
,256) as checksum from #tempstgDimGIPolicyNCD;


create table #stgdimgipolicyncd AS
select * from (select a.* , case when a.dml_ind='D' and b.dml_ind<>'D' then 1 when a.dml_ind in('I','U') AND b.dml_ind='D' THEN 1 when a.checksum <> coalesce(b.checksum,'1') and coalesce(b.active_record_ind,'Y')='Y' then 1 else 0 end as changed_rec_check from #hashStgDimGiPolicyncd a left outer join #dimpolicyncd b on a.business_key = b.business_key ) where changed_rec_check =1;

				

INSERT INTO el_eds_def.dimpolicyncd (
	source_app_code
	,source_data_set
	,dml_ind
	,record_created_date
	,record_updated_date
	,record_created_by
	,record_updated_by
	,record_eff_from_date
	,record_eff_to_date
	,active_record_ind
	,policy_ncd_uuid
	,business_key
	,policy_uuid 
	,policy_id 
	,policy_no 
	,product_code
	,ncd_policy_id
	,ncd_id
	,policy_ncd_level
	,policy_ncd_value
	,policy_ncd_status 
	,policy_ncd_status_desc
	,ncd_level
	,ncd_value
	,ncd_status
	,ncd_status_desc
	,ncd_verify_status			
	,ncd_in_policy_no
	,ncd_in_reg_no 
	,ncd_in_expiry_date
	,ncd_verify_status_desc	
	,ncd_in_company  
	,ncd_out_company
	,ncd_protection 
	,ncd_protection_waiver
	)
SELECT
	'MANUAL' AS source_app_code,
	'MANUAL' AS source_data_set,
	'I' AS dml_ind,
	GETDATE() AS record_created_date,
	GETDATE() AS record_updated_date,
	'EDS' AS record_created_by,
	'EDS' AS record_updated_by,
	CAST('1900-01-01 00:00:00.000000' AS timestamp) AS record_eff_from_date,
	CAST('9999-12-31 00:00:00.000000' AS timestamp) AS record_eff_to_date,
	'Y' as active_record_ind
	,-1 as policy_ncd_uuid
	,-1 as business_key
	,-1 as policy_uuid 
	,-1 as policy_id 
	,'unknown' as policy_no 
	,'unknown' as product_code
	,-1 as ncd_policy_id
	,-1 as ncd_id
	,-1 as policy_ncd_level
	,0 as policy_ncd_value
	,-1 as policy_ncd_status 
	,'unknown' as policy_ncd_status_desc
	,-1 as ncd_level
	, 0 as ncd_value
	,-1 as ncd_status
	,'unknown' as ncd_status_desc
	,-1 as ncd_verify_status
	,'unknown' as ncd_in_policy_no
	,'unknown' as ncd_in_reg_no 
	,'9999-12-31' as ncd_in_expiry_date
	,'unknown' as ncd_verify_status_desc 
	,'unknown' as ncd_in_company  
	,'unknown' as ncd_out_company
	,'N' as ncd_protection 
	,'N' as ncd_protection_waiver
WHERE (
	SELECT COUNT(1) FROM el_eds_def.dimpolicyncd WHERE policy_ncd_uuid = -1
	) = 0;				

DELETE FROM el_eds_def_stg.stgdimpolicyncd WHERE source_app_code = 'EBGI';
INSERT INTO el_eds_def_stg.stgdimpolicyncd (
	source_app_code
	,source_data_set
	,dml_ind
	,record_created_date
	,record_updated_date
	,record_created_by
	,record_updated_by
	,record_eff_from_date
	,record_eff_to_date
	,active_record_ind
	,checksum
	,policy_ncd_uuid
	,business_key
	,policy_uuid 
	,policy_id 
	,policy_no 
	,product_code
	,ncd_policy_id
	,ncd_id
	,policy_ncd_level
	,policy_ncd_value
	,policy_ncd_status 
	,policy_ncd_status_desc
	,ncd_level
	,ncd_value
	,ncd_status
	,ncd_status_desc
	,ncd_verify_status			
	,ncd_in_policy_no
	,ncd_in_reg_no 
	,ncd_in_expiry_date
	,ncd_verify_status_desc	
	,ncd_in_company  
	,ncd_out_company
	,ncd_protection 
	,ncd_protection_waiver
	)
select 
	source_app_code
	,source_data_set
	,dml_ind
	,record_created_date
	,record_updated_date
	,record_created_by
	,record_updated_by
	,record_eff_from_date
	,record_eff_to_date
	,active_record_ind
	,checksum
	,policy_ncd_uuid
	,business_key
	,policy_uuid 
	,policy_id 
	,policy_no 
	,product_code
	,ncd_policy_id
	,ncd_id
	,policy_ncd_level
	,policy_ncd_value
	,policy_ncd_status 
	,policy_ncd_status_desc
	,ncd_level
	,ncd_value
	,ncd_status
	,ncd_status_desc
	,ncd_verify_status			
	,ncd_in_policy_no
	,ncd_in_reg_no 
	,ncd_in_expiry_date
	,ncd_verify_status_desc	
	,ncd_in_company  
	,ncd_out_company
	,ncd_protection 
	,ncd_protection_waiver
from #stgdimgipolicyncd;

drop table if exists #v_rundate;
drop table if exists #tb_t_ncd_relation_hist;
drop table if exists #tb_t_ncd_level_define_hist;
drop table if exists #tb_t_ncd_hist;
drop table if exists #tb_t_ncd_protection_hist;
drop table if exists #dimpolicy;
drop table if exists #PKPrimary_stg_driver;
drop table if exists #PKPrimary_driver;
drop table if exists #PKPrimary_stg;
drop table if exists #PKPrimary_total;
drop table if exists #PKPrimary;
drop table if exists #tempstgDimGIPolicyNCD;
drop table if exists #hashStgDimGiPolicyncd;
drop table if exists #stgdimgipolicyncd;

END;