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
import json,base64
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

def process_messages(ti):
    messages = ti.xcom_pull(task_ids="subscribe_message")

    if not messages:
        print("No messages received.")
        return

    for msg in messages:
        encoded_data = msg['message'].get('data')
        if encoded_data:
            decoded_data = base64.b64decode(encoded_data).decode('utf-8')
            json_data = json.loads(decoded_data)
            print(json_data)  # 여기에서 JSON 데이터를 저장하거나 처리 가능


save_to_file = PythonOperator(
    task_id="save_messages_to_file",
    python_callable=process_messages,
    provide_context=True,
    dag=dag,
)

subscribe_task >> save_to_file
