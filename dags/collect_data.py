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
                file.write("2000-01-01 00:00:00")
                last_value = datetime.strptime("2000-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
        else:
            with open(last_value_path,encoding="utf-8") as file:
                last_value_str = file.read().strip()
                last_value = datetime.strptime(last_value_str, '%Y-%m-%d %H:%M:%S')
        url = f"http://192.168.56.40:8000/orders/id/{int(last_value.timestamp())}"
        with open(last_value_path, "w", encoding="utf-8") as file:
            file.write(last_value.strftime('%Y-%m-%d %H:%M:%S'))
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
    def write_last_value():
        last_value_path = "/opt/airflow/logs/last_value.txt"
        url = f"http://192.168.56.40:8000/orders/latest"
        response = requests.get(url)
        data = response.json()["log_time"]
        logger.info(f"Last Value: {data}")
        with open(last_value_path,"w",encoding="utf-8") as file:
            file.write(data)

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
            ack_id = msg['ack_id']
            publish_time = msg['message']['publish_time']
            ordering_key = msg['message']['ordering_key']
            load_timestamp = datetime.now()
            insert_data = PostgresOperator(
                task_id="postgres_insert",
                postgres_conn_id="postgres-conn",
                sql = """
                    INSERT INTO pubsub.olist_pubsub (
                        ack_id,timestamp,log_time,
                        order_id, customer_id, order_status, order_purchase_timestamp,
                        order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,
                        order_estimated_delivery_date, payment_sequential, payment_type,
                        payment_installments, payment_value, order_item_id, product_id, seller_ids,
                        shipping_limit_date, price, freight_value,
                        publish_time, ordering_key
                    ) VALUES (
                        %s,%s,%s,%s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s,%s, %s
                    )
                    ON CONFLICT DO NOTHING;
                    """,
                parameters = (
                    ack_id,load_timestamp,
                    decoded_data.get("log_time", None),
                    decoded_data.get("order_id", None),
                    decoded_data.get("customer_id", None),
                    decoded_data.get("order_status", None),
                    decoded_data.get("order_purchase_timestamp", None),
                    decoded_data.get("order_approved_at", None),
                    decoded_data.get("order_delivered_carrier_date", None),
                    decoded_data.get("order_delivered_customer_date", None),
                    decoded_data.get("order_estimated_delivery_date", None),
                    decoded_data.get("payment_sequential", None),
                    decoded_data.get("payment_type", None),
                    decoded_data.get("payment_installments", None),
                    decoded_data.get("payment_value", None),
                    decoded_data.get("order_item_id", None),
                    decoded_data.get("product_id", None),
                    json.dumps(decoded_data.get("seller_ids", None)),
                    decoded_data.get("shipping_limit_date", None),
                    decoded_data.get("price", None),
                    decoded_data.get("freight_value", None),
                    publish_time,
                    ordering_key
                )
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
        gcp_conn_id="google-cloud",
    )

    postgres_task = PythonOperator(
        task_id = "save_to_postgres",
        python_callable = save_to_postgres,
        provide_context = True
    )

    last_value = PythonOperator(
        task_id = "write_last_value",
        python_callable = write_last_value,
        provide_context = True
    )
    trigger_next_run = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="publish_to_pubsub",
        wait_for_completion=False,
    )


    data = get_order_data_after_last_value()
    data >> publish_task >> subscribe_task >> postgres_task >> last_value >> trigger_next_run
publish_to_pubsub_dag = publish_to_pubsub()
