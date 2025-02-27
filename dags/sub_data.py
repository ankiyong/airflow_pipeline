from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator,PubSubPullOperator
from airflow.decorators import task, dag
from airflow.models import XCom
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json,col
from datetime import datetime, timedelta
import os
import json
# from concurrent.futures import TimeoutError
# from queue import Queue
# import threading

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
    "pubsub_to_spark",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
)

subscribe_task = PubSubPullOperator(
    task_id='subscribe_message',
    subscription="order_data-sub",
    project_id='data-streaming-olist',
    max_messages=10,
    gcp_conn_id="google_cloud_default"
)

def save_messages_to_file(**kwargs):
    ti = kwargs["ti"]
    messages = ti.xcom_pull(task_ids="subscribe_message")

    if not messages:
        return "No messages received"

    parsed_messages = [json.loads(msg["message"]["data"]) for msg in messages]

    with open("/opt/airflow/test.json", "w") as f:
        json.dump(parsed_messages, f)

save_to_file = PythonOperator(
    task_id="save_messages_to_file",
    python_callable=save_messages_to_file,
    provide_context=True,
    dag=dag,
)

subscribe_task >> save_to_file
