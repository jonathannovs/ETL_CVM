
from pyspark.sql import functions as f
from pyspark.sql.window import Window as W
from pyspark.sql import SparkSession
import boto3
import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Transform:
    def __init__(self):
        self.spark = SparkSession.builder \
                        .appName("Teste PySpark") \
                        .master("spark://spark-master:7077") \
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
                        .getOrCreate()
        self.s3 = boto3.client(
            "s3",
            endpoint_url="http://localstack:4566", 
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1"
        )
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", "http://localstack:4566")
        hadoop_conf.set("fs.s3a.access.key", "test")
        hadoop_conf.set("fs.s3a.secret.key", "test")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


    def read_s3_files(self):
        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("encoding", "latin1") \
                .option("sep", ";") \
                .option("inferSchema", "true") \
                .csv("s3a://s3-cvm-fii/raw/*.csv")
            logging.info('ok \u2705')
        except Exception as e:
            print(f'\u270c{e}')

        return df

    def transform_data(self,df):
        if df is None:
            print("DataFrame de entrada está vazio.")
            return None

        columns_rename = {
                    "TP_FUNDO_CLASSE": "tipo_fundo",
                     "CNPJ_FUNDO_CLASSE": "cnpj_fundo",
                     "DT_COMPTC": "data_referencia",
                     "NR_COTST": "qtd_cotistas",
                     "RESG_DIA": "valor_resgates",
                     "CAPTC_DIA": "valor_aplicacoes",
                     "VL_QUOTA": "cota",
                     "VL_TOTAL": "valor_carteira",
                     "VL_PATRIM_LIQ":'pl_fundo'
                    }

        for old_name, new_name in columns_rename.items():
            df = df.withColumnRenamed(old_name, new_name)

        df = (df
            .filter(f.col('TP_FUNDO_CLASSE')=='FI')
            .withColumn('cnpj_fundo',f.regexp_replace(f.col('cnpj_fundo'), r'[./-]', ''))
            .withColumn('cota_dia_anterior',
                            f.lag(f.col('cota')).over(W.partitionBy(f.col('cnpj_fundo')).orderBy(f.col('data_referencia'))))
            .withColumn("variacao_cota_dia",
                f.when(
                    (f.col("cota_dia_anterior").isNotNull()) & (f.col("cota_dia_anterior") != 0),
                        f.round(((f.col("cota") - f.col("cota_dia_anterior")) / f.col("cota_dia_anterior")) * 100,4)))
            .withColumn("ano", f.year(f.col("data_referencia")))
            .withColumn("mes",f.month(f.col("data_referencia")))
            .withColumn("net",
                    f.col("valor_aplicacoes") - f.col("valor_resgates"))
            .withColumn("pl_d1",
                        f.lag(f.col("pl_fundo")).over(W.partitionBy(f.col("cnpj_fundo"))
                                                        .orderBy(f.col("data_referencia")))
                        )
            .withColumn('pnl',f.col('pl_fundo') - f.col('pl_d1') - f.col('net'))
            .withColumn("dt_ingest", f.current_date())
            .select(
                'cnpj_fundo',
                'pl_fundo',
                'cota',
                'qtd_cotistas',
                'valor_aplicacoes',
                'valor_resgates',
                'net',
                'pnl',
                'valor_carteira',
                'data_referencia',
                'variacao_cota_dia',
                'data_referencia',
                'mes',
                'ano',
                'dt_ingest')
                    
        ).orderBy('data_referencia')

        return df
    
    # def transform_teste(self,df):
    #     df = df.limit(5)
    #     df.show()
    #     return df
    
    def upload_stage(self,df,prefix):
        if df is None:
            print("DataFrame de entrada está vazio. Nada será escrito.")
            return
        df.write.mode("overwrite").parquet(f"s3a://s3-cvm-fii/{prefix}/")
        logging.info(f" \u2705 Dados escritos com sucesso em s3a://s3-cvm-fii/{prefix}/")































