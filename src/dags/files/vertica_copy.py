from typing import Dict, List
from airflow.models.connection import Connection
import vertica_python


def get_conn_info(*, conn: Connection):
    return {
        "host": conn.host,
        "port": conn.port,
        "user": conn.login,
        "password": conn.password,
        "database": conn.schema,
    }


def sql_execute(*, sql: str, conn_info: Dict[str, str]):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


def copy_from_local(
    *,
    from_csv_path: str,
    to_db: str,
    columns: List[str],
    conn: Connection,
):
    columns_list = ",".join(columns)

    sql = f"""
        COPY {to_db} ( {columns_list} )
        FROM LOCAL '{from_csv_path}'
        DELIMITER ','
        ENCLOSED BY '"'
        NO ESCAPE
        REJECTED DATA AS TABLE {to_db}_rej
        ; 
    """

    conn_info = get_conn_info(conn=conn)

    sql_execute(sql=sql, conn_info=conn_info)