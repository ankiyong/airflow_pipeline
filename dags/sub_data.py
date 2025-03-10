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

PROJECT_ID = "data-streaming-olist"
SUBSCRIPTION_NAME = "order_data-sub"

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
        task_instance.xcom_push(key="query_results",value=result)
        return "save_to_json"
    return "end_task"

def message_cnt():
    f = open("/opt/airflow/logs/publish_last_value.txt",'r')
    publish_last_value = f.read()
    return publish_last_value

def save_to_csv(**context):
    data = context['task_instance'].xcom_pull(key="query_results")
    csv_file_path = "/opt/airflow/logs/xcom_data.csv"
    with open(csv_file_path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(data)

publish_last_value = message_cnt()

get_data = PostgresOperator(
    task_id = "postgres_check",
    postgres_conn_id = "olist_postgres_conn",
    parameters={"publish_last_value": publish_last_value},
    sql = f"""
        SELECT
            *
        FROM
            pubsub.olist_pubsub
        WHERE
            publish_time > TO_TIMESTAMP('{publish_last_value}', 'YYYY-MM-DD HH24:MI:SS.MS')
        """
)

save_to_csv = PythonOperator(
    task_id = "save_to_json",
    python_callable = save_to_csv,
    provide_context = True
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
postgres_task = PythonOperator(
    task_id = "get_data",
    python_callable = message_cnt,
    provide_context = True
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
branch_task >> [save_to_csv, end_task]
save_to_csv >> spark_process
