from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

dag = DAG(
    dag_id="trigger_superset_dynamic_dag_v2",
    default_args={"owner": "airflow"},
    start_date=days_ago(2),
    schedule_interval=timedelta(minutes=1),
    tags=['example'],
)

def trigger(context, dag_run_obj):
    dag_run_obj.payload = {
        "sql": context["dag_run"].conf["sql"],
        "table_name": context["dag_run"].conf["table_name"]
    }
    return dag_run_obj

trigger = TriggerDagRunOperator(
    task_id="trigger_dagrun",
    trigger_dag_id="dynamic_superset_queries_task_v2",  # Ensure this equals the dag_id of the DAG to trigger
    python_callable=trigger,
    dag=dag,
)
