from pyspark.sql import SparkSession
import json,time,os,subprocess

if __name__ == "__main__":
    file_path_sec = "/opt/spark/data/xcom_data.json"
    if os.path.exists(file_path_sec):
        print("################# 존재합니다. spark 시작합니다 #################")
        spark = SparkSession.builder.appName("DataProcessing").getOrCreate()
        with open(file_path_sec, 'r') as f:
            json_data = json.load(f)
            data = []
            for i in json_data:
                i_replace = i.replace("'",'"')
                data.append(json.loads(i_replace))
            df = spark.createDataFrame(data)
            df.show()
            table = "data-streaming-olist.olist_dataset.olist_orders"
            df.write \
                .format("bigquery") \
                .option("table",table) \
                .option("temporaryGcsBucket", "your-temp-bucket") \
                .mode("append") \
                .save()
    else:
        print("################# 존재하지 않습니다 #################")
