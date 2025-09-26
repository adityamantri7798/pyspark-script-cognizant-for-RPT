BEGIN;
SET TIMEZONE = 'Singapore';

CREATE TABLE #tempstgtbrptgimtwklyclmunreadmessage AS 
    SELECT 
        source_app_code,
		    source_data_set,
        dml_ind,
        CASE 
            WHEN latest_record_created_date IS NULL 
            THEN record_created_date 
            ELSE latest_record_created_date 
        END AS record_created_date,
        record_updated_date,
        record_created_by,
        record_updated_by,
        record_eff_from_date,
        CASE 
            WHEN dml_ind <> 'D' AND uuid IS NULL 
                THEN date_trunc('second', to_timestamp('9999-12-31', 'yyyy-MM-dd'))
            WHEN dml_ind <> 'D' AND uuid IS NOT NULL AND rnk = 1
                THEN date_trunc('second', to_timestamp('9999-12-31', 'yyyy-MM-dd'))
            WHEN dml_ind = 'D' 
                THEN record_eff_from_date
            ELSE latest_record_eff_from_date 
        END AS record_eff_to_date,
        CASE 
            WHEN dml_ind <> 'D' AND uuid IS NULL
                THEN 'Y'
            WHEN dml_ind <> 'D' AND uuid IS NOT NULL AND rnk = 1  
                THEN 'Y'
            WHEN dml_ind = 'D' 
                THEN 'N'
            ELSE 'N'
        END AS active_record_ind,
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
        ,checksum
        ,table_type
        ,latest_record_eff_from_date
        ,rnk
    FROM (
        SELECT 
            temp.*,
            ROW_NUMBER() OVER (
                PARTITION BY temp.uuid 
                ORDER BY temp.record_eff_from_date DESC
            ) AS rnk,
            LAG(record_eff_from_date) OVER (
                PARTITION BY temp.uuid 
                ORDER BY temp.record_eff_from_date DESC
            ) AS latest_record_eff_from_date,
            LEAD(record_created_date) OVER (
                PARTITION BY business_key  
                ORDER BY record_eff_from_date DESC
            ) AS latest_record_created_date 
        FROM (
            SELECT 
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
                ,checksum
                ,'TEMP' as table_type 
            FROM cl_rpt_stg_temp.stg_tb_rpt_gi_mt_wkly_clm_unread_message
            UNION 
            (SELECT source_app_code,
			        source_data_set,
					dml_ind,
					record_created_date,
					record_updated_date,
					record_created_by,
					record_updated_by,
					record_eff_from_date,
					record_eff_to_date,
					active_record_ind,
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
					,checksum
					,'HIST' as table_type 
      			FROM cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message b
      			WHERE b.business_key IN (
      				SELECT business_key 
      				FROM cl_rpt_stg_temp.stg_tb_rpt_gi_mt_wkly_clm_unread_message a 
      				WHERE a.record_eff_from_date <> b.record_eff_from_date
      			)
      			AND b.active_record_ind = 'Y'
      			AND b.source_app_code = 'EBGI'
				AND b.source_data_set = 'EBGI')
      				) temp
      			) temp;



--Merging the temp table with final table

MERGE INTO cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message
USING #tempstgtbrptgimtwklyclmunreadmessage temp 
    ON cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message.business_key = temp.business_key 
    AND cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message.record_eff_from_date = temp.record_eff_from_date 
    AND cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message.source_app_code = 'EBGI'
	AND cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message.source_data_set = 'EBGI'
WHEN MATCHED THEN 
    UPDATE SET
        record_updated_date = temp.record_updated_date,
        record_eff_to_date = CASE 
            WHEN cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message.active_record_ind = 'Y' AND temp.rnk != 1 
            THEN temp.latest_record_eff_from_date 
            ELSE cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message.record_eff_to_date 
        END,
        active_record_ind = CASE 
            WHEN cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message.active_record_ind = 'Y' AND temp.rnk != 1 
            THEN temp.active_record_ind 
            ELSE cl_rpt_stg.tb_rpt_gi_mt_wkly_clm_unread_message.active_record_ind 
        END,
	     case_id = temp.case_id
	    ,object_id = temp.object_id
	    ,claim_no = temp.claim_no
	    ,message_subject = temp.message_subject 
	    ,message_read_flag = temp.message_read_flag
	    ,message_create_date = temp.message_create_date
	    ,message_urgency = temp.message_urgency
	    ,claim_type_desc = temp.claim_type_desc
	    ,officer_code  = temp.officer_code
	    ,officer_name = temp.officer_name
	    ,superior_code = temp.superior_code 
	    ,superior_name = temp.superior_name 
	    ,message_id = temp.message_id
      ,checksum = temp.checksum
WHEN NOT MATCHED THEN 
    INSERT (
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
        ,checksum
    )
    VALUES (
        temp.source_app_code,
		temp.source_data_set,
        temp.dml_ind,
        temp.record_created_date,
        temp.record_updated_date,
        temp.record_created_by,
        temp.record_updated_by,
        temp.record_eff_from_date,
        temp.record_eff_to_date,
        temp.active_record_ind,
        temp.uuid,
        temp.business_key,
        temp.case_id,
	    temp.object_id,
	    temp.claim_no,
	    temp.message_subject,  
	    temp.message_read_flag,
	    temp.message_create_date,
	    temp.message_urgency, 
	    temp.claim_type_desc, 
	    temp.officer_code,  
	    temp.officer_name,
	    temp.superior_code, 
	    temp.superior_name,
	    temp.message_id,
      temp.checksum
    );


DROP TABLE IF EXISTS #tempstgtbrptgimtwklyclmunreadmessage; 
END;