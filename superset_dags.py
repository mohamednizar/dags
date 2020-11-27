from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
import os

dag_params = {
    'owner': 'airflow',
    'dag_id': 'superset_queries',
    'start_date':datetime(2020, 4, 20),
    'schedule_interval': timedelta(seconds=60)
}

with DAG(**dag_params) as dag:
#     dag_id= dag_id . '_table{}'.(os.path.splitext(context['dag_run'].conf['extra_json.table_name'])),
    dag_name=dag_name,
    src = MysqlHook(mysql_conn_id='openemis')
    dest = MysqlsHook(mysql_conn_id='analytics')
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    dest_conn = dest.get_conn()
    dest_cursor = dest_conn.cursor()
    cursor.execute(os.path.splitext(context['dag_run'].conf['sql']))
    dest.truncate(table=os.path.splitext(context['dag_run'].conf['extra_json.table_name']))
    dest.insert_rows(table=os.path.splitext(context['dag_run'].conf['extra_json.table_name']), rows=cursor)

