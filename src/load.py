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

    def __init__(self, spark: SparkSession, host:str, database:str, user:str, password:str):
        self.spark = spark
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def create_table(self,filepath:str):
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

    def insert_data(self, schema:str, tables:list = None):

        paths = {
        "metricas": "/stage/metricas/",
        "fundos": "/stage/fundos/"
        }

        tables = tables or list(paths.keys())

        for table in tables:
            if table not in paths:
                logging.warning(f" Tabela '{table}' não está configurada no paths. Pulando...")
                continue

            try:
                path_stage = paths[table]
                logging.info(f"[Lendo parquet do {table} para inserir no banco...]")
                df = self.spark.read.parquet(f"{path_stage}*.parquet")

                logging.info('[FAZENDO A INSERÇÃO DOS DADOS...]')
                time.sleep(5)
                    
                df.write \
                    .mode("overwrite") \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://postgres:5432/CVM") \
                    .option("dbtable", f"{schema}.{table}") \
                    .option("user", self.user) \
                    .option("password", self.password) \
                    .save()
                logging.info(f"\u2705 ################## [DADOS de {table} INSERIDOS COM SUCESSO] ##################")
            except Exception as e:
                logging.error(f"\u274c ################## [ERRO AO INSERIR DADOS NA TABELA] {e} ##################")



