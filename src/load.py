import boto3
import logging
import psycopg2
import time 

from pyspark.sql import functions as f
from pyspark.sql.window import Window as W
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

logging.Formatter.converter = time.localtime
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LoadDw:

    def __init__(self, prefix, host, database, user, password):
        self.spark = SparkSession.builder \
                                .appName("Teste PySpark") \
                                .master("spark://spark-master:7077") \
                                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
                                .getOrCreate()
        self.s3 = boto3.client("s3",
            endpoint_url="http://localstack:4566", 
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1"
        )
        self.prefix = prefix
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def consulta_bucket(self):
        bucket_name = "s3-cvm-fii"
        response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{self.prefix}/")

        if "Contents" in response:
            print("Arquivos encontrados no bucket:")
            paths = [f"s3a://{bucket_name}/{obj['Key']}" for obj in response["Contents"]]
            for path in paths:
                print(f"- {path}")
        else:
            print( 'bucket vazio')

    def delete_files(self,path):
        bucket = self.s3.Bucket(f'{path}')
        bucket.objects.all().delete()
        logging.warning('################## [ARQUIVOS DELETADOS DO BUCKET] ##################')

    def create_table(self,filepath):
        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()

            with open(filepath, 'r') as f:
                sql = f.read()

            cursor.execute(sql)
            conn.commit()
            logging.info("\u2705 ################## [TABELA CRIADA COM SUCESSO] ##################")
        except Exception as e:
            logging.error(f"\u274c ################## [ERRO AO CRIAR TABELA] {e} ##################")
            raise
        finally:
            cursor.close()
            conn.close()


    def insert_data(self):
        try:
            df_parquet = self.spark.read.parquet(f"s3a://s3-cvm-fii/{self.prefix}/*.parquet")
            df_parquet.show()
            df_parquet.write.jdbc(
                url="jdbc:postgresql://postgres:5432/CVM",
                table="cvm.fundos",
                mode="overwrite",
                properties={
                    "user": self.user,
                    "password": self.password,
                    "driver": "org.postgresql.Driver"})
            logging.info("\u2705 ################## [DADOS INSERIDOS COM SUCESSO] ##################")
        except Exception as e:
            logging.error(f"\u274c ################## [ERRO AO INSERIR DADOS NA TABELA] {e} ##################")
            raise


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
