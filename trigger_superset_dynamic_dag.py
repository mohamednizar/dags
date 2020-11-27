from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import json

dag = DAG(
    dag_id="trigger_superset_dynamic_dag",
    default_args={"owner": "airflow"},
    start_date=days_ago(2),
    schedule_interval="@once",
    tags=['example'],
)

def trigger(context, dag_run_obj):
    dag_run_obj.payload = {
        "sql": context["dag_run"].conf["sql"],
        "table_name": json.load(context["dag_run"].conf["extra_json"])["table_name"]
    }
    return dag_run_obj

trigger = TriggerDagRunOperator(
    task_id="test_trigger_dagrun",
    trigger_dag_id="dynamic_superset_queries",  # Ensure this equals the dag_id of the DAG to trigger
    python_callable=trigger,
    dag=dag,
)
