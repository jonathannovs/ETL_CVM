# src/transform.py
import boto3
#from pyspark.sql import SparkSession


class Transform:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            endpoint_url="http://localstack:4566",  # ou "http://localhost:4566" se estiver fora do container
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1"
        )

    def run(self):
        bucket_name = "s3-cvm-fii"
        s3_base = f"s3a://{bucket_name}/"

        response = self.s3.list_objects_v2(Bucket=bucket_name)

        if "Contents" in response:
            print("Arquivos encontrados no bucket:")
            paths = [f"{s3_base}{obj['Key']}" for obj in response["Contents"]]
            for path in paths:
                print(f"- {path}")
        else:
            print("Nenhum arquivo encontrado no bucket.")


###### job spark 

# class Transform:
#     def __init__(self):
#         self.spark = SparkSession.builder \
#             .appName("Teste PySpark") \
#             .master("spark://spark-master:7077") \
#             .getOrCreate()

#     def run(self):
#         data = [("Alice", 25), ("Bob", 30), ("Carol", 27)]
#         df = self.spark.createDataFrame(data, ["Nome", "Idade"])
#         df.show()

#     def stop(self):
#         self.spark.stop()
