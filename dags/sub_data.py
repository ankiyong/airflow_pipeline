from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
from airflow.decorators import task, dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import os,logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
last_value_path = "/opt/airflow/logs/publish_last_value.txt"

@dag(schedule_interval="*/5 * * * *",start_date=datetime.now,catchup=False)
def spark_and_gcs_dag():
    @task
    def publish_last_value() -> str:
        if not os.path.exists(last_value_path):
            publish_val = "2000-01-01 00:00:00.000"
            with open(last_value_path, "w", encoding="utf-8") as f:
                f.write(last_value_path)
        else:
            with open(last_value_path, "r", encoding="utf-8") as f:
                publish_val = f.read().strip()
        return publish_val

    # 마지막 값 읽기 태스크
    publish_value = publish_last_value()

    # Postgres에서 publish_value 이후 데이터를 조회
    get_data = PostgresOperator(
        task_id="postgres_check",
        postgres_conn_id="olist_postgres_conn",
        trigger_rule="all_success",
        parameters={"publish_last_value": "{{ ti.xcom_pull(task_ids='publish_last_value') }}"},
        sql="""
            SELECT *
            FROM pubsub.olist_pubsub
            WHERE log_time > TO_TIMESTAMP(%(publish_last_value)s, 'YYYY-MM-DD HH24:MI:SS.MS')
        """
    )

    @task.branch
    def decide_next_task(get_data_result) -> str:
        if get_data_result and len(get_data_result[0]) > 0:
            return "spark_process"
        return "end_task"

    branch = decide_next_task(get_data.output)

    # Spark 작업 실행 태스크
    spark_process = SparkKubernetesOperator(
        task_id="spark_process",
        trigger_rule="all_success",
        depends_on_past=True,
        retries=3,
        application_file="olist_spark.yaml",
        namespace="default",
        kubernetes_conn_id="kubernetes-conn-default",
        do_xcom_push=False,
    )

    # 분기하지 않을 경우 진행되는 더미 태스크
    end_task = DummyOperator(
        task_id="end_task"
    )

    # 분기 후 합류를 위한 DummyOperator (필요 시 사용)
    merge = DummyOperator(
        task_id="merge",
        trigger_rule="none_failed_or_skipped"
    )

    # 태스크 의존성 구성
    publish_value >> get_data >> branch
    branch >> spark_process >> merge
    branch >> end_task >> merge

spark_and_gcs_dag_instance = spark_and_gcs_dag()
