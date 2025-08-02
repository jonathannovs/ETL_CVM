
import boto3
import logging
import time 

logging.Formatter.converter = time.localtime
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LoadDw:

    def __init__(self):
        self.s3 = boto3.client("s3",
            endpoint_url="http://localstack:4566", 
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1"
        )

    def consulta_bucket(self):
        bucket_name = "s3-cvm-fii"
        response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix="stage/")

        if "Contents" in response:
            print("Arquivos encontrados no bucket:")
            paths = [f"s3a://{bucket_name}/{obj['Key']}" for obj in response["Contents"]]
            for path in paths:
                print(f"- {path}")
        else:
            print( 'bucket vazio')


# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("load_dw").getOrCreate()

# df = spark.read.parquet("/src/data.parquet")  # Parquet em volume mapeado

# df.write.jdbc(
#     url="jdbc:postgresql://postgres:5432/CVM",
#     table="tabela_destino",
#     mode="overwrite",
#     properties={
#         "user": "JONANOV",
#         "password": "admin",
#         "driver": "org.postgresql.Driver"
#     }
# )
