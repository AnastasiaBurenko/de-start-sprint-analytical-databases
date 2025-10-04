INSERT INTO STV202509119__DWH.s_auth_history(hk_l_user_group_activity,user_id_from, event, event_dt, load_dt, load_src)
SELECT 
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.event_datetime,
	now() AS load_dt,
	's3' AS load_src
FROM STV202509119__STAGING.group_log gl
LEFT JOIN STV202509119__DWH.h_groups hg ON gl.group_id = hg.group_id
LEFT JOIN STV202509119__DWH.h_users hu ON gl.user_id = hu.user_id
LEFT JOIN STV202509119__DWH.l_user_group_activity luga ON hg.hk_group_id = luga.hk_group_id 
													   AND hu.hk_user_id = luga.hk_user_id;