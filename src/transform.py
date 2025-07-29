# src/transform.py
import boto3
from pyspark.sql import SparkSession

class Transform:
    # def __init__(self):
    #     self.spark = SparkSession.builder \
    #         .appName("Teste PySpark") \
    #         .master("spark://spark-master:7077") \
    #         .getOrCreate()

    # def run(self):
    #     data = [("Alice", 25), ("Bob", 30), ("Carol", 27)]
    #     df = self.spark.createDataFrame(data, ["Nome", "Idade"])
    #     df.show()

    # def stop(self):
    #     self.spark.stop()
import boto3

# Cliente S3 apontando para o LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",  # ou "http://localstack:4566" se estiver dentro do container
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

bucket_name = "s3-cvm-fii"

response = s3.list_objects_v2(Bucket=bucket_name)

if "Contents" in response:
    print("Arquivos encontrados no bucket:")
    for obj in response["Contents"]:
        print(f"- {obj['Key']}")
else:
    print("Nenhum arquivo encontrado no bucket.")
