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
        interval = 10000
        url = f"http://192.168.28.3:8000/orders/id/{int(last_value)}/{int(last_value)+interval}"
        with open(last_value_path, "w", encoding="utf-8") as file:
            file.write(f"{int(last_value)+interval}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Data fetched successfully: {len(data)} records")
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
                enable_message_ordering= True
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
            ack_id = msg['ack_id']
            delivery_attempt = msg['delivery_attempt']
            publish_time = msg['message']['publish_time']
            ordering_key = msg['message']['ordering_key']
            load_timestamp = datetime.now()
            insert_data = PostgresOperator(
                task_id = "postgres_insert",
                postgres_conn_id = "olist_postgres_conn",
                sql =
                    f"""
                        INSERT INTO pubsub.olist_pubsub (
                            ack_id,delivery_attempt,timestamp,
                            order_id, customer_id, order_status, order_purchase_timestamp,
                            order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,
                            order_estimated_delivery_date, payment_sequential, payment_type,
                            payment_installments, payment_value, order_item_id, product_id, seller_id,
                            shipping_limit_date, price, freight_value,
                            publish_time,ordering_key
                        ) VALUES (
                            '{ack_id}',
                            '{delivery_attempt}',
                            '{load_timestamp}',
                            '{decoded_data["order_id"] }',
                            '{decoded_data["customer_id"] }',
                            '{decoded_data["order_status"] }',
                            '{decoded_data["order_purchase_timestamp"] }',
                            '{decoded_data["order_approved_at"] }',
                            '{decoded_data["order_delivered_carrier_date"] }',
                            '{decoded_data["order_delivered_customer_date"] }',
                            '{decoded_data["order_estimated_delivery_date"] }',
                            '{decoded_data["payment_sequential"] }',
                            '{decoded_data["payment_type"] }',
                            '{decoded_data["payment_installments"] }',
                            '{decoded_data["payment_value"] }',
                            '{decoded_data["order_item_id"] }',
                            '{decoded_data["product_id"] }',
                            '{decoded_data["seller_id"] }',
                            '{decoded_data["shipping_limit_date"] }',
                            '{decoded_data["price"] }',
                            '{decoded_data["freight_value"] }',
                            '{publish_time}',
                            '{ordering_key}'
                        )
                    """,
                )
            insert_data.execute(context={})


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
        gcp_conn_id="google_cloud_default",
    )

    postgres_task = PythonOperator(
        task_id = "save_to_postgres",
        python_callable = save_to_postgres,
        provide_context = True
)
    trigger_next_run = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="publish_to_pubsub",
        wait_for_completion=False,
    )


    data = get_order_data_after_last_value()
    data >> publish_task >> subscribe_task >> postgres_task >> trigger_next_run
publish_to_pubsub_dag = publish_to_pubsub()
