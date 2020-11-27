from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
import logging

default_args = {
    "owner": "airflow",
    "provide_context": True
}

dag = DAG(
    dag_id='dynamic_superset_queries_dag_generator',
    default_args={"owner": "airflow", "provide_context" : True},
    start_date=days_ago(1),
    schedule_interval="@once"
)

process_creator_task = PythonOperator(
        task_id="process_creator",
        python_callable=generate_dags_for_queries,
    )


def generate_dags_for_queries(**context):
    table_name = format(context["dag_run"].conf["table_name"])
    dag_name = 'dynamic_superset_queries_dag_generator_{}'.table_name
    dag = DAG(
        dag_name,
        schedule_interval=timedelta(minutes=5),
        start_date=days_ago(1),
        default_args=default_args
    )
    dag_task = PythonOperator(task_id="running_queries_{}".table_name, python_callable=create_or_update_table, dag=dag)
    return dag


def insert_or_update_table(**context):
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
