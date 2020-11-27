from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
import logging
import os
import pprint

dag = DAG(
    dag_id='dynamic_superset_queries',
    default_args={"owner": "airflow"},
    start_date=days_ago(1),
    schedule_interval="@once",
)


def create_or_update_table(**kwargs):
    try:
        logging.info('trying the task')
        pprint(kwargs);
        sql = kwargs["drag_run"].conf["sql"]
        table_name = kwargs["drag_run"].conf["table_name"]
        logging.info('connecting to source')
        src = MySqlHook(mysql_conn_id='openemis')
        logging.info('connecting to destination')
        dest = MySqlHook(mysql_conn_id='analytics')
        src_conn = src.get_conn()
        cursor = src_conn.cursor()
        dest_conn = dest.get_conn()
        cursor.execute(sql)
        dest.truncate(table=table_name)
        dest.insert_rows(table=table_name, rows=cursor)
    except Exception as e3:
        logging.error('Dag failed , please refer the logs more details')
        logging.exception(e3)
        logging.exception(kwargs)


run_this = PythonOperator(task_id="run_this", python_callable=create_or_update_table, dag=dag)
