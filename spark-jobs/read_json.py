from pyspark.sql import SparkSession
import json,os
from pyspark.sql.functions import col,to_timestamp,unix_timestamp,floor

if __name__ == "__main__":
    file_path_sec = "/opt/spark/data/xcom_data.json"
    if os.path.exists(file_path_sec):
        print("################# 존재합니다. spark 시작합니다 #################")
        gcs_bucket = "olist_buckets"
        parquet_path = f"gs://{gcs_bucket}/orders/orders.parquet"
        spark = (
            SparkSession.builder
            .appName("pyspark-gcs-connection")
            .master("local[*]")
            .config("spark.jars", "/opt/spark/data/gcs-connector-hadoop3-2.2.9-shaded.jar")
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/spark/data/key.json")
            .getOrCreate()
            )

        with open(file_path_sec, 'r') as f:
            json_data = json.load(f)
            data = []
            for i in json_data:
                i_replace = i.replace("'",'"')
                data.append(json.loads(i_replace))
            df = spark.createDataFrame(data)
            df = df.withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
                    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
                    .withColumn("time_diff_seconds", unix_timestamp(col("order_delivered_customer_date")) - unix_timestamp(col("order_purchase_timestamp"))) \
                    .withColumn("delevery_time",floor(col("time_diff_seconds")/3600))

            df = df.drop("id","order_estimated_delivery_date","order_approved_at","order_delivered_carrier_date")
            df = df.na.drop()
            df.show()
            df.write.format("parquet").save(parquet_path)
    else:
        print("################# 존재하지 않습니다 #################")
