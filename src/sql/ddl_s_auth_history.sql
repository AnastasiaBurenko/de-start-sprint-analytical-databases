CREATE TABLE STV202509119__DWH.s_auth_history
(
    hk_l_user_group_activity BIGINT NOT NULL CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV202509119__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from int,
    event varchar(100),
    event_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
