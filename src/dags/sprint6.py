from datetime import datetime
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
import boto3

from files import vertica_copy as vertica

vertica_conn = BaseHook.get_connection(conn_id="de_vertica")
staging_schema = "STV202509119__STAGING"

AWS_ACCESS_KEY_ID = "YCAJEiyNFq4wiOe_eMCMCXmQP"
AWS_SECRET_ACCESS_KEY = "YCP1e96y4QI8OmcB4Eaf4q0nMHwhmtvGbDTgBeqS"


def fetch_s3_file(bucket: str, key: str, filename: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=filename
    )


@dag(
    schedule_interval="@daily",
    start_date=datetime(2022, 7, 1),
    catchup=False,
)
def sprint6():
    start = EmptyOperator(task_id="start")

    bucket_files = [
        ('sprint6', 'users.csv', '/data/users.csv'),
        ('sprint6', 'groups.csv', '/data/groups.csv'),
        ('sprint6', 'dialogs.csv', '/data/dialogs.csv'),
        ('sprint6', 'group_log.csv', '/data/group_log.csv'),
    ]

    fetch_tasks = []
    for bucket, key, filename in bucket_files:
        fetch_task = PythonOperator(
            task_id=f'fetch_{key.replace(".csv", "")}',  # Уникальный task_id
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': bucket, 'key': key, 'filename': filename},
        )
        fetch_tasks.append(fetch_task)

    with TaskGroup(group_id="staging") as staging:
        @task
        def load_users():
            vertica.copy_from_local(
                from_csv_path="/data/users.csv",
                to_db=f"{staging_schema}.users",
                columns=[
                    "id",
                    "chat_name ENFORCELENGTH",
                    "registration_dt",
                    "country ENFORCELENGTH",
                    "age",
                ],
                conn=vertica_conn,
            )

        @task
        def load_groups():
            vertica.copy_from_local(
                from_csv_path="/data/groups.csv",
                to_db=f"{staging_schema}.groups",
                columns=[
                    "id",
                    "admin_id",
                    "group_name ENFORCELENGTH",
                    "registration_dt",
                    "is_private",
                ],
                conn=vertica_conn,
            )

        @task
        def load_dialogs():
            vertica.copy_from_local(
                from_csv_path="/data/dialogs.csv",
                to_db=f"{staging_schema}.dialogs",
                columns=[
                    "message_id",
                    "message_ts",
                    "message_from",
                    "message_to",
                    "message ENFORCELENGTH",
                    "group_filler FILLER NUMERIC",
                    "message_group AS group_filler::INT",
                ],
                conn=vertica_conn,
            )

        @task
        def load_group_log():
            vertica.copy_from_local(
                from_csv_path="/data/group_log.csv",
                to_db=f"{staging_schema}.group_log",
                columns=[
                    "group_id",
                    "user_id",
                    "user_id_from",
                    "event ENFORCELENGTH",
                    "event_datetime",
                ],
                conn=vertica_conn,
            )

        load_users_task = load_users()
        load_groups_task = load_groups()
        load_dialogs_task = load_dialogs()
        load_group_log_task = load_group_log()

        load_users_task >> load_groups_task >> load_dialogs_task >> load_group_log_task

    end = EmptyOperator(task_id="end")

    # Определяем полную последовательность выполнения
    start >> fetch_tasks >> staging >> end


dag = sprint6()
