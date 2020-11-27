from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime , timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
import os

dag = DAG(
     dag_id='dynamic_superset_queries_{}',
     default_args={"owner": "airflow"},
     start_date=days_ago(1),
     schedule_interval=timedelta(seconds=60),
)

def create_or_update_table(**context):
    sql = context["drag_run"].conf["sql"]
    table_name = context["drag_run"].conf["table_name"]
    src = MysqlHook(mysql_conn_id='openemis')
    dest = MysqlsHook(mysql_conn_id='analytics')
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    dest_conn = dest.get_conn()
    dest_cursor = dest_conn.cursor()
    cursor.execute(query_params.sql)
    dest.truncate(table=query_params.table_name)
    dest.insert_rows(table=query_params.table_name, rows=cursor)
    
    
run_this =  PythonOperator(task_id="run_this", python_callable=create_or_update_table, dag=dag)

bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Table to run this: $table_name"',
    env={'sql': '{{ dag_run.conf["sql"] if dag_run else "" }}','table_name': '{{ dag_run.conf["table_name"] if dag_run else "" }}'},
    dag=dag,
)
