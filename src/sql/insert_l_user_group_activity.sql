INSERT INTO STV202509119__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
SELECT DISTINCT 
    hash(hu.hk_user_id, hg.hk_group_id),
    hu.hk_user_id,
    hg.hk_group_id,
    now() AS load_dt,
    's3' AS load_src
FROM STV202509119__STAGING.group_log g
LEFT JOIN STV202509119__DWH.h_users hu ON g.user_id = hu.user_id
LEFT JOIN STV202509119__DWH.h_groups hg ON g.group_id = hg.group_id
WHERE hash(hu.hk_user_id, hg.hk_group_id) NOT IN (
    SELECT hk_l_user_group_activity 
    FROM STV202509119__DWH.l_user_group_activity
    );
