from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable
import requests,json
from datetime import datetime, timedelta
import os



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
        file.write(f"{last_value+10}")
    response = requests.get(url)
    if response.status_code == 200:
        print("Connect Success")
        return response.json()
    

with DAG(
    'data_pipeline',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple data_pipeline DAG',
    schedule_interval='1 * * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = PythonOperator(
        task_id='get-order-data',
        python_callable=get_order_data_after_last_value
    )

t1
