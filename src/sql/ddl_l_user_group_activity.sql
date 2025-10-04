CREATE TABLE STV202509119__DWH.l_user_group_activity
(
    hk_l_user_group_activity BIGINT PRIMARY KEY,
    hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_user REFERENCES STV202509119__DWH.h_users (hk_user_id),
    hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_group REFERENCES STV202509119__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
