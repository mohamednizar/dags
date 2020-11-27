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
    dag_id='dynamic_superset_queries_dag_generator',
    default_args={"owner": "airflow", "provide_context": True},
    start_date=days_ago(1),
    schedule_interval="@once"
)


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
        logging.error('Table update is failed, please refer the logs more details')
        logging.exception(context)
        logging.exception(e3)


def generate_dags_for_queries(**context):
    """
    access the  payload params passed to the DagRun conf attribute.
    :param context: The execution context
    :type context: dict
    """
    try:
        table_name = format(context["dag_run"].conf["table_name"])
        dag_id = f"dynamic_superset_queries_dag_generator_{table_name}"
        new_dag = DAG(
            dag_id=dag_id,
            default_args={"owner": "airflow"},
            start_date=days_ago(1),
            schedule_interval=timedelta(minutes=5),
            catchup=False
        )
        task_name = f"running_queries_{table_name}"
        with new_dag:
            dag_task = PythonOperator(task_id=task_name, python_callable=insert_or_update_table,
                                      dag=new_dag)
        globals()[dag_id] = new_dag
        return new_dag
    except Exception as e3:
        logging.error('Dag creation failed , please refer the logs more details')
        logging.exception(context)
        logging.exception(e3)


with dag:
    t1 = PythonOperator(
        task_id='generate_dags_for_queries',
        python_callable=generate_dags_for_queries)
