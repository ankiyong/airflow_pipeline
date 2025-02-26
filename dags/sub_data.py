from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator,PubSubPullOperator
from airflow.decorators import task, dag
from airflow.models import XCom
import requests,json
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from google.cloud import pubsub_v1
import json
from concurrent.futures import TimeoutError
from queue import Queue
import threading

@dag(schedule_interval=None,start_date=days_ago(1),catchup=False)
def subscribe_from_pubsub():
    def get_data(queue:Queue):
        project_id = "data-streaming-olist"
        subscription_id = "order-data-sub"
        timeout = 5.0

        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_id)

        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            print(f"Received {message}.")
            queue.put(message.data.decode())
            message.ack()

        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        with subscriber:
            try:
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()
                streaming_pull_future.result()
        return queue
    queue = Queue()
    def fetch_data():
        get_data(queue)

# spark = SparkSession.builder \
#         .appName("OlistOrderData") \
#         .getOrCreate()
    @task
    def data_sub():
        data_thread = threading.Thread(target=fetch_data)
        data_thread.start()

    data = get_order_data_after_last_value()
    publish_data(data)

publish_to_pubsub_dag = publish_to_pubsub()
# def data_process():
#     merged_df = ''
#     while True:
#         if queue:
#             message = queue.get()
#             try:
#                 data = json.loads(message)
#                 df = spark.read.json(spark.sparkContext.parallelize([data]))
#                 # if type(merged_df) == "DataFrame":
#                 merged_df = merged_df.unionAll(df)
#             except Exception as e:
#                 print(f"Error processing message: {e}")

#     merged_df.show()
# # 데이터를 처리하는 부분을 비동기로 실행
# process_thread = threading.Thread(target=data_process)
# process_thread.start()

# data_thread.join()
# process_thread.join()

