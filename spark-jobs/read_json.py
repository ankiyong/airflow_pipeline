from pyspark.sql import SparkSession
import json,time,os,subprocess

if __name__ == "__main__":
    file_path_sec = "/opt/spark/data/xcom_data.json"
    if os.path.exists(file_path_sec):
        print("################# 존재합니다. spark 시작합니다 #################")
        spark = SparkSession.builder.appName("DataProcessing") \
                .getOrCreate()

        with open(file_path_sec, 'r') as f:
            json_data = json.load(f)
            data = []
            for i in json_data:
                i_replace = i.replace("'",'"')
                data.append(json.loads(i_replace))
            df = spark.createDataFrame(data)
            df.show()
            json_list = [row.asDict() for row in df.collect()]
    else:
        print("################# 존재하지 않습니다 #################")
