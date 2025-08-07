
from pyspark.sql import functions as f
from pyspark.sql.window import Window as W
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Transform:

    def __init__(self, spark: SparkSession):

        self.spark = spark

        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", "http://localstack:4566")
        hadoop_conf.set("fs.s3a.access.key", "test")
        hadoop_conf.set("fs.s3a.secret.key", "test")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.committer.name", "directory")
        
    def read_s3_files(self,prefix):

        schema = StructType([
            StructField("TP_FUNDO_CLASSE", StringType(), True),
            StructField("CNPJ_FUNDO_CLASSE", StringType(), True),
            StructField("ID_SUBCLASSE", StringType(),True),
            StructField("DT_COMPTC", DateType(), True),
            StructField("NR_COTST", IntegerType(), True),
            StructField("VL_QUOTA", DoubleType(), True),
            StructField("VL_PATRIM_LIQ", DoubleType(), True),
            StructField("CAPTC_DIA", DoubleType(), True),
            StructField("RESG_DIA", DoubleType(), True),
            StructField("VL_TOTAL", DoubleType(), True)
        ])

        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("encoding", "latin1") \
                .option("sep", ";") \
                .schema(schema) \
                .csv(f"s3a://s3-cvm-fii/{prefix}/*.csv")
            logging.info(' ############### \u2705 [ARQUIVOS LIDOS] #################')
        except Exception as e:
            print(f'\u274c{e}')
        return df

    def transform_data(self,df):
        if df is None:
            print("DataFrame de entrada está vazio.")
            return None
        df = (df
            .filter(f.col('TP_FUNDO_CLASSE')=='FI')
            .withColumn('CNPJ_FUNDO_CLASSE',f.regexp_replace(f.col('CNPJ_FUNDO_CLASSE'), r'[./-]', ''))
            .withColumn('ano',f.year(f.col('DT_COMPTC')))
            .select(
                f.col('CNPJ_FUNDO_CLASSE').alias('cnpj_fundo'),
                f.col('NR_COTST').alias('qtd_cotistas'),
                f.col('RESG_DIA').alias('valor_resgates'),
                f.col('CAPTC_DIA').alias('valor_aplicacoes'),
                f.col('VL_QUOTA').alias('cota'),
                f.col('VL_TOTAL').alias('valor_carteira'),
                f.col('VL_PATRIM_LIQ').alias('pl_fundo'),
                f.col('DT_COMPTC').alias('data_referencia'),
                f.col('ano'),
                f.current_date().alias('dt_ingest'))
        )

        logging.info("\u2705 ################## [DATAFRAME ENVIADO PARA UPLOAD] ###################")
        return df

    def transform_teste(self,df):
        #df = df.limit(5)
        df = df.filter(f.col("Ano") == 2025)

        logging.info("\u2705 ################### [DATAFRAME <TESTE> ENVIADO PARA UPLOAD] ##################")
        return df

    def upload_stage(self,df,prefix):
        if df is None:
            logging.warning("\u274c DataFrame de entrada está vazio. Nada será escrito.")
            return None

        try:
            df.write \
                .mode("overwrite") \
                .parquet(f"s3a://s3-cvm-fii/{prefix}/")
            logging.info(f" \u2705 #################### [DADOS SALVOS COM SUCESSO EM s3a://s3-cvm-fii/{prefix}/] ################## ")

        except Exception as e:
            logging.error(f"\u274c [ERRO AO SALVAR PARQUET NO S3]: {e}")



























