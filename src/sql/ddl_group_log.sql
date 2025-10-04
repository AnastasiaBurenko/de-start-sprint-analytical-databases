CREATE table STV202509119__STAGING.group_log
(
    group_id int,
    user_id int,
    user_id_from int,
    event varchar(10),
    event_datetime timestamp
)
ORDER BY group_id, user_id
SEGMENTED BY hash(group_id) all nodes
PARTITION BY event_datetime::date
GROUP BY calendar_hierarchy_day(event_datetime::date, 3, 2);