
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import sys
import os
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
        
    def read_s3_files(self,prefix:str):

        select_cols = ["TP_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "DT_COMPTC", "VL_QUOTA","VL_PATRIM_LIQ",'CAPTC_DIA','RESG_DIA','NR_COTST']
        map_columns = {
                        "TP_FUNDO":'TP_FUNDO_CLASSE',
                        "CNPJ_FUNDO":"CNPJ_FUNDO_CLASSE"}

        def padronizar_colunas(df, mapa):
            for col in df.columns:
                if col in mapa:
                    df = df.withColumnRenamed(col, mapa[col])
            return df

        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("encoding", "latin1") \
                .option("sep", ";") \
                .option("inferSchema", "false")\
                .csv(f"s3a://s3-cvm-fii/{prefix}/*.csv")\
                .select(*select_cols)
            df = padronizar_colunas(df, map_columns)
            logging.info(' ############### \u2705 [ARQUIVOS LIDOS] #################')

        except Exception as e:
            print(f'\u274c{e}')
        return df

    def transform_data(self,df):
        if df is None:
            print("DataFrame de entrada está vazio.")
            return None
        
        df = (df
            .withColumn('CNPJ_FUNDO_CLASSE',f.regexp_replace(f.col('CNPJ_FUNDO_CLASSE'), r'[./-]', ''))
            .withColumn('DT_COMPTC',f.col('DT_COMPTC').cast(DateType()))
            .withColumn('VL_QUOTA',f.round(f.col('VL_QUOTA').cast(DoubleType()),2))
            .withColumn('VL_PATRIM_LIQ',f.round(f.col('VL_PATRIM_LIQ').cast(DoubleType()),2))
            .withColumn('CAPTC_DIA',f.round(f.col('CAPTC_DIA').cast(DoubleType()),2))
            .withColumn('RESG_DIA',f.round(f.col('RESG_DIA').cast(DoubleType()),2))
            .withColumn('NR_COTST', f.col('NR_COTST').cast(IntegerType()))
            .withColumn('ano',f.year(f.col('DT_COMPTC')))
            .withColumn('id_fund_date',f.concat(f.col('CNPJ_FUNDO_CLASSE'),f.date_format(f.col('DT_COMPTC'), 'yyyyMMdd')))
            .drop('TP_FUNDO_CLASSE')
            .select(
                f.col('id_fund_date'),
                f.col('CNPJ_FUNDO_CLASSE').alias('cnpj_fundo'),
                f.col('RESG_DIA').alias('valor_resgates'),
                f.col('CAPTC_DIA').alias('valor_aplicacoes'),
                f.col('VL_QUOTA').alias('cota'),
                f.col('VL_PATRIM_LIQ').alias('pl_fundo'),
                f.col('DT_COMPTC').alias('data_referencia'),
                f.col('NR_COTST').alias('qtd_cotistas'),
                f.col('ano'),
                f.current_date().alias('dt_ingest')
            )
    ).drop_duplicates(['id_fund_date'])

        logging.info("\u2705 ################## [DATAFRAME ENVIADO PARA UPLOAD NO S3] ###################")
        return df

    def calculate_metricas(self, df):
        if df is None:
            print("DataFrame de entrada está vazio.")
            return None
        
        df = df.select('id_fund_date','cnpj_fundo','valor_resgates','valor_aplicacoes','cota','pl_fundo','data_referencia','ano')

        janela_fundo = w.partitionBy(f.col('cnpj_fundo')).orderBy('data_referencia')

        df = (df
            .withColumn('cota_anterior',f.lag('cota').over(janela_fundo))
            .withColumn('pct_rentabilidade_diaria', f.round(((f.col('cota') / f.col('cota_anterior'))-1)*100,4))
            .withColumn('net', f.col('valor_aplicacoes') - f.col('valor_resgates'))
            .withColumn('pl_anterior', f.lag('pl_fundo').over(janela_fundo))
            .withColumn('pnl', f.round(f.col('pl_fundo') - f.col('pl_anterior') - f.col('net'),4))
            .withColumn('dt_ingest',f.current_date())
            .drop('cota_anterior','pl_anterior')
            )
        
        logging.info("\u2705 ################## [DATAFRAME  METRICAS ENVIADO PARA UPLOAD NO S3] ###################")
        return df

    def transform_teste(self,df):
        df = df.limit(1000)
        #df = df.filter(f.col("Ano") == 2025)

        logging.info("\u2705 ################### [DATAFRAME <TESTE> ENVIADO PARA UPLOAD] ##################")
        return df

    def upload_stage(self,df, tipo_df:str = None):

        if df is None:
            logging.warning("\u274c DataFrame de entrada está vazio. Nada será escrito.")
            return None
        
        paths = {
        "metricas": "/stage/metricas/",
        "fundos": "/stage/fundos/",
        "full": None
        }

        try:
            if tipo_df == 'full':
                for key in ['metricas','fundos']:
                    path_stage = paths[key]
                    os.makedirs(path_stage, exist_ok=True)
                    try:
                        df.write.mode("overwrite").parquet(path_stage)
                        logging.info(f" \u2705 #################### [DADOS SALVOS COM SUCESSO NO STAGE {path_stage}] ################## ")
                    except Exception as e:
                        logging.error(f"\u274c [ERRO AO SALVAR PARQUET NO STAGE {path_stage}]: {e}")
            else:
                path_stage = paths.get(tipo_df)
                if path_stage is None:
                    logging.warning(f"\u274c Tipo '{tipo_df}' inválido para salvar.")
                    return None
                os.makedirs(path_stage, exist_ok=True)
                df.write.mode("overwrite").parquet(path_stage)
                logging.info(f"\u2705 Dados salvos com sucesso no stage {path_stage}")
        except Exception as e:
            logging.error(f"\u274c Erro ao salvar parquet no stage: {e}")
            sys.exit(1)






























