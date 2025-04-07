from airflow import DAG
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator,PubSubPullOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task, dag
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import requests,json,base64,logging
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
LAST_VALUE = Variable.get("LAST_VALUE_PATH")
DB_HOST = Variable.get("DB_HOST")
@dag(schedule_interval=None,start_date= datetime.now(),catchup=False)
def publish_to_pubsub():
    @task
    def get_order_data_after_last_value() -> list:
        if not os.path.exists(LAST_VALUE):
            with open(LAST_VALUE, "w", encoding="utf-8") as file:
                file.write("2000-01-01 00:00:00")
                last_value = datetime.strptime("2000-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
        else:
            with open(LAST_VALUE,encoding="utf-8") as file:
                last_value_str = file.read().strip()
                last_value = datetime.strptime(last_value_str, '%Y-%m-%d %H:%M:%S')

        url = f"{DB_HOST}/orders/id/{int(last_value.timestamp())}"
        with open(LAST_VALUE, "w", encoding="utf-8") as file:
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
            return []
    
    @task
    def write_last_value():
        url = f"{DB_HOST}/orders/latest"
        try:
            response = requests.get(url)
            response.raise_for_status()
            latest_log_time = response.json().get("log_time")
            if latest_log_time:
                logger.info(f"Updating last value to: {latest_log_time}")
                with open(LAST_VALUE, "w", encoding="utf-8") as file:
                    file.write(latest_log_time)
            else:
                logger.warning("Response에 최신 log_time이 없습니다.")
        except requests.RequestException as e:
            logger.error(f"Failed to update last value: {e}")
    @task
    def publish_data(ti):
        data = ti.xcom_pull(task_ids="get_order_data_after_last_value")
        if not data:
            logger.info("데이터가 존재하지 않습니다.")
            return None

        hook = PubSubHook(gcp_conn_id="google-cloud")
        batch_size = 1000
        topic = 'olist_dataset'

        data_list = []
        for d in data:
            data_list.append({"data": json.dumps(d).encode("utf-8")})
            if len(data_list) == batch_size:
                hook.publish(
                    project_id='olist-data-engineering',
                    topic=topic,
                    messages=data_list
                )
                data_list = []

        if data_list:
            hook.publish(
                project_id='olist-data-engineering',
                topic=topic,
                messages=data_list
            )

    def convert_empty_string_to_null(data):
        if isinstance(data, str) and data == '':
            return None
        return data
    def save_to_postgres(messages: list) -> None:
        """
        Pub/Sub에서 가져온 메시지를 Postgres에 저장합니다.
        """
        if not messages:
            logger.info("저장할 메시지가 없습니다.")
            return

        pg_hook = PostgresHook(postgres_conn_id="postgres-conn")
        insert_sql = """
            INSERT INTO pubsub.olist_pubsub (
                ack_id, timestamp, log_time,
                order_id, customer_id, order_status, order_purchase_timestamp,
                order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,
                order_estimated_delivery_date, payment_sequential, payment_type,
                payment_installments, payment_value, order_item_id, product_id, seller_ids,
                shipping_limit_date, price, freight_value,
                publish_time, ordering_key
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING;
        """

        for msg in messages:
            try:
                decoded_bytes = base64.b64decode(msg['message']['data'])
                record = json.loads(decoded_bytes.decode('utf-8'))
            except Exception as ex:
                logger.error(f"메시지 디코딩 에러: {ex}")
                continue

            # 빈 문자열은 None으로 변환
            record = {key: convert_empty_string_to_null(value) for key, value in record.items()}
            ack_id = msg.get('ack_id')
            publish_time = msg.get('message', {}).get('publish_time')
            ordering_key = msg.get('message', {}).get('ordering_key')
            load_timestamp = datetime.now()

            params = (
                ack_id,
                load_timestamp,
                record.get("log_time"),
                record.get("order_id"),
                record.get("customer_id"),
                record.get("order_status"),
                record.get("order_purchase_timestamp"),
                record.get("order_approved_at"),
                record.get("order_delivered_carrier_date"),
                record.get("order_delivered_customer_date"),
                record.get("order_estimated_delivery_date"),
                record.get("payment_sequential"),
                record.get("payment_type"),
                record.get("payment_installments"),
                record.get("payment_value"),
                record.get("order_item_id"),
                record.get("product_id"),
                json.dumps(record.get("seller_ids")),
                record.get("shipping_limit_date"),
                record.get("price"),
                record.get("freight_value"),
                publish_time,
                ordering_key
            )
            try:
                pg_hook.run(insert_sql, parameters=params)
            except Exception as e:
                logger.error(f"Postgres 저장 에러: {e}")
            


    order_data = get_order_data_after_last_value()
    publish = publish_data(order_data)

    subscribe_messages = PubSubPullOperator(
        task_id='subscribe_message',
        subscription="order-data-subscribe",
        project_id="olist-data-engineering",
        max_messages=10000,
        gcp_conn_id="google-cloud",
    )

    save_messages = save_to_postgres(subscribe_messages.output)
    update_last_value = write_last_value()

    trigger_next_run = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="publish_to_pubsub",
        wait_for_completion=False,
    )

    order_data >> publish >> subscribe_messages >> save_messages >> update_last_value >> trigger_next_run


dag_instance = publish_to_pubsub()