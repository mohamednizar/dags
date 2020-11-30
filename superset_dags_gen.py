import requests
import os
import json
import pprint
from bs4 import BeautifulSoup
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
import logging

superset_host = os.environ['SUPERSET_HOST']
superset_username = os.environ['SUPERSET_USERNAME']
superset_password = os.environ['SUPERSET_PASSWORD']
superset_port = os.environ['SUPERSET_PORT']

# set up session for auth
s = requests.Session()
base_url = f"http://{superset_host}:{superset_port}"
login_form = s.get(f"{base_url}/login")

# get Cross-Site Request Forgery protection token
soup = BeautifulSoup(login_form.text, 'html.parser')
csrf_token = soup.find('input', {'id': 'csrf_token'})['value']

# login the given session
s.post(login_form, data=dict(username=superset_username, password=superset_password, csrf_token=csrf_token))

# fetch all saved queries
saved_queries = s.get(f"{base_url}/savedqueryviewapi/api/read").text

dag = DAG(
    dag_id='dynamic_superset_queries_task',
    default_args={"owner": "airflow", "provide_context": True},
    start_date=days_ago(1),
    schedule_interval=timedelta(minutes=10)
)

START = DummyOperator(
    task_id='START'
)

END = DummyOperator(
    task_id='END'
)


def insert_or_update_table(**kwargs):
    """
     access the  payload params passed to the DagRun conf attribute.
     :param context: The execution context
     :type context: dict
     """
    try:
        table_name = kwargs['schedule_info']['table_name']
        sql = kwargs['sql']
        logging.info('trying the task')
        logging.info('connecting to source')
        src = MySqlHook(mysql_conn_id=kwargs['schedule_info']['schema'])
        logging.info(f"Remotely received sql of {sql}")
        logging.info(f"Remotely received sql of {table_name}")
        logging.info('connecting to destination')
        dest = MySqlHook(mysql_conn_id='analytics')
        src_conn = src.get_conn()
        cursor = src_conn.cursor()
        cursor.execute(sql)
        dest.insert_rows(table=table_name, rows=cursor, replace=True)
    except Exception as e3:
        logging.error('Table update is failed, please refer the logs more details')
        logging.exception(context)
        logging.exception(e3)


def generate_dags_for_queries(dag_id, schedule,  default_args, saved_queries  ):
    try:
        logging.info(f"DAG is:{dag_id}")
        with DAG(dag_id, default_args=default_args, schedule_interval=schedule, catchup=False) as new_dag:
            task_name = f"running_queries_{table_name}".upper()

            dummy_start = DummyOperator(
                task_id='dummy_start'
            )

            dummy_end = DummyOperator(
                task_id='dummy_end'
            )

            dag_task = PythonOperator(
                task_id=task_name,
                python_callable=insert_or_update_table,
                op_kwargs=saved_queries,
                provide_context=True
            )
            dummy_start >> dag_task
            dag_task >> dummy_end
            logging.info('Task is:{}'.task_name)
            return new_dag
    except Exception as e3:
        logging.error('Dag creation failed , please refer the logs more details')
        logging.exception(context)
        logging.exception(e3)


number_of_queries = (json.load(saved_queries))
for n in number_of_queries:
    saved_query = saved_queries[n]
    data = json.load(saved_query["extra_json"])
    table_name = data['schedule_info']['output_table']
    dag_id = f"saved_queries_update{table_name}".lower()

    default_args = {'owner': 'airflow',
                    'start_date': data['schedule_info']['start_date'],
                    'end_date': data['schedule_info']['end_date'],
                    }
    schedule = timedelta(minutes=10)
    globals[dag_id] = generate_dags_for_queries(dag_id,
                                                schedule,
                                                default_args,
                                                saved_queries)
