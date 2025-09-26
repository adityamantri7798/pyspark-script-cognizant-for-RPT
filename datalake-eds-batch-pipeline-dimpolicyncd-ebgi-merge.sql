BEGIN;

--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
SET TIMEZONE = 'Singapore';

CREATE TABLE #stgdimpolicyncd AS

SELECT source_app_code
	,source_data_set
	,dml_ind
	,CASE 
		WHEN latest_record_created_date IS NULL
			THEN record_created_date
		ELSE latest_record_created_date
		END AS record_created_date
	,record_updated_date
	,record_created_by
	,record_updated_by
	,record_eff_from_date
	,CASE 
		WHEN dml_ind <> 'D'
			AND policy_ncd_uuid IS NULL
			THEN date_trunc('second', to_timestamp('9999-12-31', 'yyyy-MM-dd'))
		WHEN dml_ind <> 'D'
			AND policy_ncd_uuid IS NOT NULL
			AND rnk = 1
			THEN date_trunc('second', to_timestamp('9999-12-31', 'yyyy-MM-dd'))
		WHEN dml_ind = 'D'
			THEN record_eff_from_date
		ELSE latest_record_eff_from_date
		END AS record_eff_to_date
	,CASE 
		WHEN dml_ind <> 'D'
			AND policy_ncd_uuid IS NULL
			THEN 'Y'
		WHEN dml_ind <> 'D'
			AND policy_ncd_uuid IS NOT NULL
			AND rnk = 1
			THEN 'Y'
		WHEN dml_ind = 'D'
			THEN 'N'
		ELSE 'N'
		END AS active_record_ind
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
	,table_type
	,latest_record_eff_from_date
	,checksum
	,rnk
FROM (
	SELECT TEMP.*
		,ROW_NUMBER() OVER (
			PARTITION BY TEMP.policy_ncd_uuid ORDER BY TEMP.record_eff_from_date DESC
			) AS rnk
		,LAG(record_eff_from_date) OVER (
			PARTITION BY TEMP.policy_ncd_uuid ORDER BY TEMP.record_eff_from_date DESC
			) AS latest_record_eff_from_date
		,LEAD(record_created_date) OVER (
			PARTITION BY business_key ORDER BY record_eff_from_date DESC
			) AS latest_record_created_date
	FROM (
		SELECT source_app_code
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
			,checksum
			,'TEMP' AS table_type
		FROM el_eds_def_stg.stgdimpolicyncd
		WHERE source_app_code = 'EBGI'
		
		UNION
		
		(
			SELECT source_app_code
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
				,checksum
				,'HIST' AS table_type
			FROM el_eds_def.dimpolicyncd b
			WHERE business_key IN (
					SELECT business_key
					FROM el_eds_def_stg.stgdimpolicyncd a
					WHERE a.record_eff_from_date <> b.record_eff_from_date
						AND a.source_app_code = 'EBGI'
					)
				AND b.active_record_ind = 'Y'
				AND b.source_app_code = 'EBGI'
			)
		) TEMP
	) TEMP;

MERGE
INTO el_eds_def.dimpolicyncd USING #stgdimpolicyncd TEMP ON el_eds_def.dimpolicyncd.business_key = TEMP.business_key
	AND el_eds_def.dimpolicyncd.record_eff_from_date = TEMP.record_eff_from_date
	AND el_eds_def.dimpolicyncd.source_app_code = 'EBGI' WHEN MATCHED THEN

UPDATE
SET record_updated_date = TEMP.record_updated_date
	,record_eff_to_date = CASE 
		WHEN el_eds_def.dimpolicyncd.active_record_ind = 'Y'
			AND TEMP.rnk != 1
			THEN TEMP.latest_record_eff_from_date
		ELSE el_eds_def.dimpolicyncd.record_eff_to_date
		END
	,active_record_ind = CASE 
		WHEN el_eds_def.dimpolicyncd.active_record_ind = 'Y'
			AND TEMP.rnk != 1
			THEN TEMP.active_record_ind
		ELSE el_eds_def.dimpolicyncd.active_record_ind
		END
	,policy_uuid = TEMP.policy_uuid
	,policy_id = TEMP.policy_id
	,policy_no = TEMP.policy_no
	,product_code = TEMP.product_code
	,ncd_policy_id = TEMP.ncd_policy_id
	,ncd_id = TEMP.ncd_id
	,policy_ncd_level = TEMP.policy_ncd_level
	,policy_ncd_value = TEMP.policy_ncd_value
	,policy_ncd_status = TEMP.policy_ncd_status
	,policy_ncd_status_desc = TEMP.policy_ncd_status_desc
	,ncd_level = TEMP.ncd_level
	,ncd_value = TEMP.ncd_value
	,ncd_status = TEMP.ncd_status
	,ncd_status_desc = TEMP.ncd_status_desc
	,ncd_verify_status = TEMP.ncd_verify_status
	,ncd_in_policy_no = TEMP.ncd_in_policy_no
	,ncd_in_reg_no = TEMP.ncd_in_reg_no
	,ncd_in_expiry_date = TEMP.ncd_in_expiry_date
	,ncd_verify_status_desc = TEMP.ncd_verify_status_desc
	,ncd_in_company = TEMP.ncd_in_company
	,ncd_out_company = TEMP.ncd_out_company
	,ncd_protection = TEMP.ncd_protection
	,ncd_protection_waiver = TEMP.ncd_protection_waiver
	,checksum = TEMP.checksum WHEN NOT MATCHED THEN

INSERT (
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
	,checksum
	)
VALUES (
	TEMP.source_app_code
	,TEMP.source_data_set
	,TEMP.dml_ind
	,TEMP.record_created_date
	,TEMP.record_updated_date
	,TEMP.record_created_by
	,TEMP.record_updated_by
	,TEMP.record_eff_from_date
	,TEMP.record_eff_to_date
	,TEMP.active_record_ind
	,TEMP.policy_ncd_uuid
	,TEMP.business_key
	,TEMP.policy_uuid
	,TEMP.policy_id
	,TEMP.policy_no
	,TEMP.product_code
	,TEMP.ncd_policy_id
	,TEMP.ncd_id
	,TEMP.policy_ncd_level
	,TEMP.policy_ncd_value
	,TEMP.policy_ncd_status
	,TEMP.policy_ncd_status_desc
	,TEMP.ncd_level
	,TEMP.ncd_value
	,TEMP.ncd_status
	,TEMP.ncd_status_desc
	,TEMP.ncd_verify_status
	,TEMP.ncd_in_policy_no
	,TEMP.ncd_in_reg_no
	,TEMP.ncd_in_expiry_date
	,TEMP.ncd_verify_status_desc
	,TEMP.ncd_in_company
	,TEMP.ncd_out_company
	,TEMP.ncd_protection
	,TEMP.ncd_protection_waiver
	,TEMP.checksum
	);

DROP TABLE

IF EXISTS #stgdimpolicyncd;

END;