from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

PROJECT_ID = "data-streaming-olist"
SUBSCRIPTION_NAME = "order_data-sub"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "spark_and_gcs",
    default_args=default_args,
    schedule_interval="* * * * * *",
    catchup=False,
)
def decide_next_task(**kwargs):
    result = kwargs['ti'].xcom_pull(task_ids="get_data")  # XCom에서 결과 가져오기
    if result and int(result) > 0:
        return "spark-process"
    return "end_task"

def message_cnt():
    f = open("/opt/airflow/logs/publish_last_value.txt",'r')
    publish_last_value = f.read()
    get_data = PostgresOperator(
        task_id = "postgres_check",
        postgres_conn_id = "olist_postgres_conn",
        sql = f"""
            SELECT
                cnt(order_id)
            FROM
                pubsub.olist_pubsub
            WHERE
                publish_time > TO_TIMESTAMP('{publish_last_value}', 'YYYY-MM-DD HH24:MI:SS.MS')
            """
        )
    get_data.execute(context={})

branch_task = BranchPythonOperator(
    task_id="branch_task",
    python_callable=decide_next_task,
    provide_context=True,
    dag=dag
)

end_task = DummyOperator(
    task_id="end_task",
    dag=dag
)
postgres_task = PythonOperator(
    task_id = "get_data",
    python_callable = message_cnt,
    provide_context = True
)

spark_process = SparkKubernetesOperator(
    task_id="spark-process",
    trigger_rule="all_success",
    depends_on_past=True,
    retries=3,
    application_file="olist_spark.yaml",
    namespace="default",
    kubernetes_conn_id="kubernetes-conn-default",
    do_xcom_push=False,
    dag=dag
)

postgres_task >> branch_task
branch_task >> [spark_process, end_task]
