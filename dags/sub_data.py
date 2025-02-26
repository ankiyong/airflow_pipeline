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
# from datetime import datetime, timedelta
# import os
# import json
# from concurrent.futures import TimeoutError
# from queue import Queue
# import threading

@dag(schedule_interval=None,start_date=days_ago(1),catchup=False)
def subscribe_from_pubsub():
    @task
    def subscribe_data():
        subscribe_task = PubSubPullOperator(
            task_id="sub_message",
            subscription="order_data-sub",
            project_id='data-streaming-olist',
            topic='order_data',
            max_messages=10,
        )
        subscribe_task.execute(context={})
    subscribe_data()

subscribe_dag = subscribe_from_pubsub()
