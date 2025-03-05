from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import json,base64

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

def process_messages(ti):
    messages = ti.xcom_pull(task_ids="subscribe_message")

    if not messages:
        print("No messages received.")
        return
    data = []
    for msg in messages:
        encoded_data = msg['message'].get('data')
        if encoded_data:
            decoded_data = base64.b64decode(encoded_data).decode('utf-8')
            data.append(decoded_data)
    ti.xcom_push(key="return_value",value=data)

def save_xcom_to_json(ti):
    data = ti.xcom_pull(task_ids="save_messages_to_file",key="return_value")
    file_path = "/opt/airflow/logs/xcom_data.json"
    with open(file_path,"w") as f:
        json.dump(data,f,indent=4)


subscribe_task = PubSubPullOperator(
    task_id='subscribe_message',
    subscription="order_data-sub",
    project_id='data-streaming-olist',
    max_messages=100,
    gcp_conn_id="google_cloud_default",
    dag=dag
)

process_messages = PythonOperator(
    task_id="save_messages_to_file",
    depends_on_past=True,
    python_callable=process_messages,
    provide_context=True,
    dag=dag,
)

save_to_json=PythonOperator(
    task_id="save_to_json",
    depends_on_past=True,
    python_callable=save_xcom_to_json,
    provide_context=True,
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

wait = TimeDeltaSensor(
    task_id="wait_5_seconds",
    delta=timedelta(seconds=10)
)

trigger_next_run = TriggerDagRunOperator(
    task_id="trigger_next_run",
    trigger_dag_id="spark_and_gcs",
    wait_for_completion=False,
)

subscribe_task >> process_messages >> save_to_json >> spark_process >> wait >> trigger_next_run
