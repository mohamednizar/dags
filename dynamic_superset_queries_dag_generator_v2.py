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
    dag_id='dynamic_superset_queries_dag_generator_v2',
    default_args={"owner": "airflow"},
    start_date=days_ago(1),
    schedule_interval="@once"
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag)


def generate_dags_for_queries(**context):
    """
    access the  payload params passed to the DagRun conf attribute.
    :param context: The execution context
    :type context: dict
    """
    sql = format(context["dag_run"].conf["sql"])
    table_name = format(context["dag_run"].conf["table_name"])

    def insert_or_update_table(**kwargs):
        try:
            logging.info('trying the task')
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

    try:
        table_name = format(context["dag_run"].conf["table_name"]).lower()
        dag_name = "dynamic_superset_queries_dag_{table_name}"
        logging.info('DAG is:{}'.dag_name)
        args = {"owner": "airflow", "start_date": days_ago(1)}
        with DAG(
            dag_name,
            default_args=args,
            schedule_interval='@hourly',
            catchup=False
        ) as new_dag:
            task_name = f"running_queries_{table_name}"
            dag_task = PythonOperator(
                task_id=task_name,
                python_callable=insert_or_update_table)
            logging.info('Task is:{}'.task_name)
            return new_dag
    except Exception as e3:
        logging.error('Dag creation failed , please refer the logs more details')
        logging.exception(context)
        logging.exception(e3)


with dag:
    t1 = PythonOperator(
        task_id='generate_dags_for_queries',
        python_callable=generate_dags_for_queries
    )
    start_task >> t1
    t1 >> end
