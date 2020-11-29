from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
import logging

dag = DAG(
    dag_id='dynamic_superset_queries_task_v2',
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


def create_or_update_table(**context):
    """
    access the  payload params passed to the DagRun conf attribute.
    :param context: The execution context
    :type context: dict
    """
    try:
        logging.info('trying the task')
        sql = format(context["dag_run"].conf["sql"])
        table_name = format(context["dag_run"].conf["table_name"])
        logging.info('connecting to source')
        src = MySqlHook(mysql_conn_id='openemis')
        logging.info('connecting to destination')
        print("Remotely received value of {} for key=sql".sql)
        print("Remotely received value of {} for key=table_name".table_name)
        dest = MySqlHook(mysql_conn_id='analytics')
        src_conn = src.get_conn()
        cursor = src_conn.cursor()
        dest_conn = dest.get_conn()
        cursor.execute(sql)
        dest.insert_rows(table=table_name, rows=cursor, replace=True)
    except Exception as e3:
        logging.error('Dag failed , please refer the logs more details')
        logging.exception(context)
        logging.exception(e3)


def dynamic_task(**context):
    table_name = format(context["dag_run"].conf["table_name"])
    task_name = f"create_or_update_{table_name}".lower()
    create_dynamic_task = PythonOperator(task_id=task_name, python_callable=create_or_update_table, dag=dag)
    return create_dynamic_task


with dag:
    run_this = PythonOperator(task_id="dynamic_task_create", python_callable=dynamic_task, dag=dag)
    START >> run_this
    run_this >> END
