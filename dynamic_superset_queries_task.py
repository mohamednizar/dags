import requests
import os
import json
import pprint
from bs4 import BeautifulSoup as bs
from bs4 import Comment
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
import logging

superset_url = os.environ['SUPERSET_URL']
superset_username = os.environ['SUPERSET_USERNAME']
superset_password = os.environ['SUPERSET_PASSWORD']


class UseSupersetApi:
    def __init__(self, username=None, password=None):
        self.s = requests.Session()
        self.base_url = superset_url
        self._csrf = self._getCSRF(self.url('login/'))
        self.headers = {'X-CSRFToken': self._csrf, 'Referer': self.url('login/')}
        # note: does not use headers because of flask_wtf.csrf.validate_csrf
        # if data is dict it is used as form and ends up empty but flask_wtf checks if data ...
        self.s.post(self.url('login/'),
                    data={'username': username, 'password': password, 'csrf_token': self._csrf})

    def url(self, url_path):
        return self.base_url + url_path

    def get(self, url_path):
        return self.s.get(self.url(url_path), headers=self.headers)

    def post(self, url_path, data=None, json_data=None, **kwargs):
        kwargs.update({'url': self.url(url_path), 'headers': self.headers})
        if data:
            data['csrf_token'] = self._csrf
            kwargs['data'] = data
        if json_data:
            kwargs['json'] = json_data
        return self.s.post(**kwargs)

    def _getCSRF(self, url_path):
        response = self.s.get(self.base_url)
        soup = bs(response.content, "html.parser")
        for tag in soup.find_all('input', id='csrf_token'):
            csrf_token = tag['value']
        return csrf_token


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
        json_data = json.load(kwargs["extra_json"])
        table_name = json_data['schedule_info']['table_name']
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


def generate_dags_for_queries(dag_id, schedule, default_args, saved_query):
    try:
        logging.info(f"DAG is:{dag_id}")
        with DAG(dag_id, default_args=default_args, schedule_interval=schedule, catchup=False) as new_dag:
            task_name = f"{dag_id}_update_table_task".upper()

            dummy_start = DummyOperator(
                task_id='dummy_start'
            )

            dummy_end = DummyOperator(
                task_id='dummy_end'
            )

            dag_task = PythonOperator(
                task_id=task_name,
                python_callable=insert_or_update_table,
                op_kwargs=saved_query,
                provide_context=True
            )
            dummy_start >> dag_task
            dag_task >> dummy_end
            logging.info(f"Task is:{task_name}")
            return new_dag
    except Exception as e3:
        logging.error('Dag creation failed , please refer the logs more details')
        logging.exception(context)
        logging.exception(e3)


superset = UseSupersetApi(superset_username, superset_password)
saved_queries = superset.get(url_path='/savedqueryviewapi/api/read').text
saved_queries = json.loads(saved_queries)["result"]
for saved_query in saved_queries:
    data = json.loads(saved_query['extra_json'])
    if bool(data) is True:
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
                                                saved_query)
