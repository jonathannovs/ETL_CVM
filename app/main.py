import sys
import logging 
import time
import os
from pyspark.sql import SparkSession 

sys.path.append("/src")  
from transform import Transform
from extract import ExtractCvm
from load import LoadDw

DB_NAME = 'CVM'
DB_USER = 'JONANOV'
DB_PASSWORD = 'admin'

os.makedirs("/app/logs", exist_ok=True)
logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/etl.log", mode="a"),
        logging.StreamHandler()])

def main():

    spark = SparkSession.builder \
        .appName("Pipeline CVM") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")\
        .config("spark.jars", "/opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar") \
        .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar") \
        .getOrCreate()

    lista_tabelas = ['fundos','metricas']

    logging.info('[# 1 -------- EXTRAINDO CVM ----------#]')
    time.sleep(5)

    #instanciando a classe e passando o período que se quer extrair os dados, 
    #caso não se passe as datas, irá pegar do dia 01/01/2022 até o dia atual.
    ext = ExtractCvm(bucket_name="s3-cvm-fii",start_date='2022-01-01',end_date='2023-12-01')

    #criando Bucket s3 no Localstack
    ext.create_bucket()

    #extraindo os dados de informes diários dos fundos e de informções sobre fundos
    ext.extract_info_diary(prefix='raw')
    ext.extract_infos_funds(prefix='raw_infos')

    logging.info('[# 2 -------- TRANSFORMANDO DADOS----------#]')
    time.sleep(5)

    #instanciando a classe de transfomração e e passando a configuração spark
    tr = Transform(spark)
     
    # busca os dados do bucket raw do s3 e le e concatena em csv
    df_raw = tr.read_s3_files(prefix='raw')      
    
    # recebe o df lido em csv e faz o tratamento        
    df_transformed = tr.transform_data(df_raw) 

    # recebe o df tratado e faz a junção com df com nome dos fundos
    df_join = tr.join_named_fund(df_transformed) 
    
    # recebe o df que foi tratado e faz cálculos            
    df_metricas = tr.calculate_metricas(df_join)   

    # dicionario com tabela para inserir como chave e função como valor
    tr.upload_stage({lista_tabelas[0]: df_join,
                    lista_tabelas[1]: df_metricas})

    logging.info('[# 3 -------- CARREGANDO DADOS NO DATA WAREHOUSE ----------#]')
    time.sleep(5)

    #instanciando a classe LoadDw e passando os acessos ao banco de dados como parâmetros
    load = LoadDw(spark,
                host="postgres",
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD)

    #criaçõa de tabelas
    load.create_table(filepath='/sql/create_tables.sql')

    #inserção no banco de dados
    load.insert_data(schema='cvm', tables=lista_tabelas)

    #exclusão dos arquivos parquet
    load.clean_temp_folder()

    logging.info('\u2705[#################### PIPELINE FINALIZADO #################]')
    time.sleep(2)

    spark.stop()

if __name__ == "__main__":
    main()

