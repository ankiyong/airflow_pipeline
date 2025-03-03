from pyspark.sql import SparkSession
import json,time,os,subprocess

if __name__ == "__main__":
    file_path_sec = "/opt/spark/data/xcom_data.json"
    if os.path.exists(file_path_sec):
        print("################# 존재합니다. spark 시작합니다 #################")
        spark = SparkSession.builder.appName("DataProcessing").getOrCreate()
        # with open(file_path_sec,"r",encoding="utf-8") as f:
        #     raw_data = json.load(f)
        # parsed_data = [json.loads(item) for item in raw_data]
        data = []
        with open(file_path_sec, 'r') as f:
            raw_data = json.load(f)
            parsed_data = [json.loads(item) for item in raw_data]
            df = spark.createDataFrame(parsed_data[0])
            df.show()
        # multiline_df = spark.read.option("multiline","true") .json(parsed_data)
        # multiline_df.printSchema()
        # multiline_df.show()
    else:
        print("################# 존재하지 않습니다 #################")
