from airflow import DAG
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator,PubSubPullOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task, dag
import requests,json,base64,logging
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        url = f"http://192.168.56.40:8000/orders/id/{int(last_value)}"
        with open(last_value_path, "w", encoding="utf-8") as file:
            file.write(f"{int(last_value)}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Data fetched successfully: {len(data)} records")
            print(data)
            return data
        except requests.RequestException as e:
            logger.error(f"Failed to fetch order data: {e}")
            data = []
            return data

    def publish_data(ti):
        data = ti.xcom_pull(task_ids="get_order_data_after_last_value")
        if not data:
            logger.info("데이터가 존재하지 않습니다.")
            return None

        for d in data:
            json_data = json.dumps(d).encode("utf-8")
            publish_task = PubSubPublishMessageOperator(
                task_id='publish_message',
                project_id='olist-data-engineering',
                topic='olist_dataset',
                enable_message_ordering= True,
                gcp_conn_id="google-cloud",
                messages=[{'data': json_data}]
            )
            publish_task.execute(context={})
    def convert_empty_string_to_null(data):
        if isinstance(data, str) and data == '':
            return None
        return data
    def save_to_postgres(ti):
        messages = ti.xcom_pull(task_ids="subscribe_message")
        if not messages:
            logger.info("메시지가 존재하지 않습니다.")
            return
        for msg in messages:
            encoded_data = json.loads(base64.b64decode(msg['message']['data']).decode('utf-8'))
            decoded_data = {key: convert_empty_string_to_null(value) for key, value in encoded_data.items()}
            # ack_id = msg['ack_id']
            # ordering_key = msg['message']['ordering_key']
            # load_timestamp = datetime.now()
            print(decoded_data)


    publish_task = PythonOperator(
        task_id = "publish_message",
        python_callable=publish_data,
        provide_context=True,
    )

    subscribe_task = PubSubPullOperator(
        task_id='subscribe_message',
        subscription="order-data-subscribe",
        project_id='olist-data-engineering',
        max_messages=10000,
        gcp_conn_id="google-cloud",
    )

    postgres_task = PythonOperator(
        task_id = "save_to_postgres",
        python_callable = save_to_postgres,
        provide_context = True
    )
    # trigger_next_run = TriggerDagRunOperator(
    #     task_id="trigger_next_run",
    #     trigger_dag_id="publish_to_pubsub",
    #     wait_for_completion=False,
    # )


    data = get_order_data_after_last_value()
    # data >> publish_task >> subscribe_task >> postgres_task >> trigger_next_run
    data >> publish_task >> subscribe_task >> postgres_task
publish_to_pubsub_dag = publish_to_pubsub()
