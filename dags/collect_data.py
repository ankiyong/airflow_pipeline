from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator,PubSubPullOperator
import requests,json
from datetime import datetime, timedelta
import os
from airflow.decorators import task, dag
from airflow.models import XCom

@dag(schedule_interval=None,start_date=days_ago(1),catchup=False)
def publish_to_pubsub():
    @task
    def get_order_data_after_last_value():
        last_value_path = "/opt/airflow/logs/last_value.txt"
        if not os.path.exists(last_value_path):
            with open(last_value_path, "w", encoding="utf-8") as file:
                file.write("0")
                last_value = 0
        else:
            with open(last_value_path,encoding="utf-8") as file:
                last_value = file.read()
        url = f"http://220.70.6.76:8000/orders/id/{int(last_value)}/{int(last_value)+10}"
        with open(last_value_path, "w", encoding="utf-8") as file:
            file.write(f"{int(last_value)+10}")
        response = requests.get(url)
        if response.status_code == 200:
            print("Connect Success")
            return response.json()
    @task
    def publish_data(data):
        for d in data:
            json_data = json.dumps(d).encode("utf-8")
            publish_task = PubSubPublishMessageOperator(
                task_id='publish_message',
                project_id='data-streaming-olist',
                topic='order_data',
                messages=[{'data': json_data}]
            )
            publish_task.execute(context={})
    
    data = get_order_data_after_last_value()
    publish_data(data)

publish_to_pubsub_dag = publish_to_pubsub()
# def get_order_data_after_last_value():
#     last_value_path = "/opt/airflow/logs/last_value.txt"
#     if not os.path.exists(last_value_path):
#         with open(last_value_path, "w", encoding="utf-8") as file:
#             file.write("0")
#             last_value = 0
#     else:
#         with open(last_value_path,encoding="utf-8") as file:
#             last_value = file.read()
#     url = f"http://220.70.6.76:8000/orders/id/{int(last_value)}/{int(last_value)+10}"
#     with open(last_value_path, "w", encoding="utf-8") as file:
#         file.write(f"{int(last_value)+10}")
#     response = requests.get(url)
#     if response.status_code == 200:
#         print("Connect Success")
#         return response.json()
    
# with DAG(
#     'data_pipeline',
#     default_args={
#         'depends_on_past': False,
#         'email': ['airflow@example.com'],
#         'email_on_failure': False,
#         'email_on_retry': False,
#         'retries': 5,
#         'retry_delay': timedelta(minutes=5),
#     },
#     description='A simple data_pipeline DAG',
#     schedule_interval='1 * * * *',
#     start_date=datetime(2021, 1, 1),
#     catchup=False,
#     tags=['example'],
# ) as dag:

#     get_order_data = PythonOperator(
#         task_id='get-order-data',
#         python_callable=get_order_data_after_last_value
#     )
#     publish_task = PubSubPublishMessageOperator(
#     task_id='publish_message',
#     project_id='data-streaming-olist',
#     topic='order_data',
#     messages="{{ task_instance.xcom_pull(task_ids='get-order-data', key='return_value') | tojson }}",
#     dag=dag,
# )
# get_order_data >> publish_task
