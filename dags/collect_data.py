from airflow import DAG
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator,PubSubPullOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests,json,base64
from datetime import datetime,timedelta
import os,random
from airflow.decorators import task, dag

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
        interval = 10
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
                project_id='olist-data-engineering',
                topic='olist_dataset',
                messages=[{'data': json_data}]
            )
            publish_task.execute(context={})
    def save_to_postgres(ti):
        messages = ti.xcom_pull(task_ids="subscribe_message")

        if not messages:
            print("No messages received.")
            return
        for msg in messages:
            encoded_data = msg['message'].get('data')
            ack_id = msg['ack_id']
            message = msg['message']
            delivery_attempt = msg['delivery_attempt']
            insert_data = PostgresOperator(
                task_id = "postgres_insert",
                postgres_conn_id = "olist_postgres_conn",
                sql = f"""
                    insert into olist_pubsub (ack_id,messages,delivery_attempt)
                    values ({ack_id},"{message}",{delivery_attempt})
                    """
                )
            insert_data.execute(context={})
            # if encoded_data:
            #     decoded_data = base64.b64decode(encoded_data).decode('utf-8')
            #     print(decoded_data)


    publish_task = PythonOperator(
        task_id = "publish_message",
        python_callable=publish_data,
        provide_context=True,
    )

    subscribe_task = PubSubPullOperator(
        task_id='subscribe_message',
        subscription="order-data-subscribe",
        project_id='olist-data-engineering',
        max_messages=100,
        gcp_conn_id="google_cloud_default",
    )

    postgres_task = PythonOperator(
        task_id = "save_to_postgres",
        python_callable = save_to_postgres,
        provide_context = True
    )

    data = get_order_data_after_last_value()
    data >> publish_task >> subscribe_task >> postgres_task
publish_to_pubsub_dag = publish_to_pubsub()
