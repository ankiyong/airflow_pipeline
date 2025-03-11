from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import csv

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "spark_and_gcs",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
def decide_next_task(**context):
    task_instance = context['task_instance']
    result = context['task_instance'].xcom_pull(key="return_value")
    if result and len(result[0]) > 0:
        return "save_to_json"
    return "end_task"

def publish_last_value(**context):
    f = open("/opt/airflow/logs/publish_last_value.txt",'r')
    publish_last_value = f.read()
    task_instance = context['task_instance']
    task_instance.xcom_push(key="return_value",value=publish_last_value)

publish_lastvalue = PythonOperator(
    task_id = "publish_lastvalue",
    python_callable = "publish_last_value",
    dag=dag
)

get_data = PostgresOperator(
    task_id = "postgres_check",
    postgres_conn_id = "olist_postgres_conn",
    parameters={"publish_last_value": "{{ ti.xcom_pull(task_ids='publish_lastvalue') }}"},
    sql = f"""
        SELECT
            *
        FROM
            pubsub.olist_pubsub
        WHERE
            publish_time > TO_TIMESTAMP('{publish_last_value}', 'YYYY-MM-DD HH24:MI:SS.MS')
        """
    )


branch_task = BranchPythonOperator(
    task_id="branch_task",
    python_callable=decide_next_task,
    provide_context=True,
    dag=dag
)

end_task = DummyOperator(
    task_id="end_task",
    dag=dag
)

spark_process = SparkKubernetesOperator(
    task_id="spark-process",
    trigger_rule="all_success",
    depends_on_past=True,
    retries=3,
    application_file="olist_spark.yaml",
    namespace="default",
    kubernetes_conn_id="kubernetes-conn-default",
    do_xcom_push=False,
    dag=dag
)

get_data >> branch_task
branch_task >> [spark_process, end_task]
