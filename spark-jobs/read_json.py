from pyspark.sql import SparkSession,functions as F
import json,os,datetime,shutil,time
from pyspark.sql.functions import col,to_timestamp,unix_timestamp,floor
from functools import reduce
from dotenv import load_dotenv


def spark_sess():
    spark = (
        SparkSession.builder
        .appName("pyspark-gcs-connection")
        .master("local[*]")
        .config("spark.jars", "/opt/spark/data/gcs-connector-hadoop3-2.2.9-shaded.jar")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/spark/data/key.json")
        .config("spark.driver.extraClassPath", "/opt/spark/data/postgresql-42.7.5.jar") \
        .getOrCreate()
        )
    return spark

def get_last_value(file_path):
    if not os.path.exists(file_path):
        publish_last_value = "2000-01-01 00:00:00.000"
        f = open(file_path,'w')
        f.write("2000-01-01 00:00:00.000") #최초 값 세팅
    else:
        f = open(file_path,'r')
        publish_last_value = f.read()
    return publish_last_value
def write_last_value(last_value,value):
    f = open(last_value,'w')
    f.write(value)

def get_df(sql):
    df = spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://192.168.28.3:5431/postgres") \
                .option("driver", "org.postgresql.Driver") \
                .option("query", sql) \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .load()
    return df

if __name__ == "__main__":
    source_file = "/opt/spark/data/postgresql-42.7.5.jar"
    target_dir = "/opt/spark/jars/postgresql-42.7.5.jar"
    os.system("cp /opt/spark/data/postgresql-42.7.5.jar /opt/spark/jars")
    shutil.copy(source_file, target_dir)
    last_value_file_path = "/opt/spark/data/publish_last_value.txt"
    spark = (
        SparkSession.builder
        .appName("pyspark-gcs-connection")
        .master("local[*]")
        .config("spark.jars", "/opt/spark/data/gcs-connector-hadoop3-2.2.9-shaded.jar")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/spark/data/key.json")
        .config("spark.driver.extraClassPath", "/opt/spark/data/postgresql-42.7.5.jar") \
        .getOrCreate()
        )
    publish_last_value = get_last_value(last_value_file_path)
    sql = f"SELECT * FROM pubsub.olist_pubsub WHERE publish_time > TO_TIMESTAMP('{publish_last_value}', 'YYYY-MM-DD HH24:MI:SS.MS') ORDER BY publish_time DESC"

    df = spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://192.168.28.3:5431/postgres") \
                .option("driver", "org.postgresql.Driver") \
                .option("query", sql) \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .load()
    #last value 갱신
    new_last_value = df.first()["publish_time"]
    write_last_value(last_value_file_path,str(new_last_value))

    column_name = ["price","freight_value","payment_value"]
    print("##################1")
    df = df.na.drop()
    print("##################2")
    df = reduce(lambda df, col: df.withColumn(col, F.round(df[col], 2)), column_name, df)
    print("##################3")
    df = df.withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
        .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
        .withColumn("time_diff_seconds", unix_timestamp(col("order_delivered_customer_date")) - unix_timestamp(col("order_purchase_timestamp"))) \
        .withColumn("delevery_time",floor(col("time_diff_seconds")/3600))
    print("##################4")
    df = df.drop("id","order_estimated_delivery_date","order_approved_at","order_delivered_carrier_date","delivery_attempt","ordering_key","time_diff_seconds")
    gcs_bucket = "olist_buckets"
    parquet_path = f"gs://{gcs_bucket}/orders/orders.parquet"
    print("##################5")
    df.write.mode("append").format("parquet").save(parquet_path)
