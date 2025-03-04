from airflow import DAG
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
import requests,json,uuid
from datetime import datetime
import os,random
from airflow.decorators import task, dag
from airflow.models import XCom

@dag(schedule_interval=None,start_date= datetime.now(),catchup=False)
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
        interval = random.randrange(3,30)
        url = f"http://192.168.28.3:8000/orders/id/{int(last_value)}/{int(last_value)+interval}"
        with open(last_value_path, "w", encoding="utf-8") as file:
            file.write(f"{int(last_value)+interval}")
        response = requests.get(url)
        if response.status_code == 200:
            print("Connect Success")
            return response.json()

    def publish_data(ti):
        data = ti.xcom_pull(task_ids="get_order_data_after_last_value")
        if not data:
            print("데이터가 존재하지 않습니다.")
            return None

        for d in data:
            json_data = json.dumps(d).encode("utf-8")
            publish_task = PubSubPublishMessageOperator(
                task_id='publish_message',
                project_id='data-streaming-olist',
                topic='order_data',
                messages=[{'data': json_data}]
            )
            publish_task.execute(context={})

    publish_task = PythonOperator(
        task_id = "publish_message",
        python_callable=publish_data,
        provide_context=True,
    )
    trigger_next = TriggerDagRunOperator(
        # task_id="trigger_next_run",
        trigger_dag_id="publish_to_pubsub",
        execution_date=None,  # 새로운 실행을 자동 생성
        run_id=f"manual__{uuid.uuid4()}"
        wait_for_completion=False
    )
    data = get_order_data_after_last_value()
    data >> publish_task >> trigger_next
publish_to_pubsub_dag = publish_to_pubsub()
