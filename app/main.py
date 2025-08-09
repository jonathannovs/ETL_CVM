import sys
import logging 
import time
from pyspark.sql import SparkSession 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sys.path.append("/src")  


from transform import Transform
from extract import ExtractCvm
from load import LoadDw


DB_NAME = 'CVM'
DB_USER = 'JONANOV'
DB_PASSWORD = 'admin'

def main():

    spark = SparkSession.builder \
        .appName("Pipeline CVM") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")\
        .config("spark.jars", "/opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar") \
        .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar") \
        .getOrCreate()

    lista_tabelas = ['metricas','fundos']

    logging.info('[# 1 -------- EXTRAINDO CVM ----------#]')
    time.sleep(5)

    ext = ExtractCvm(start_date=2022, bucket_name="s3-cvm-fii")
    ext.create_bucket()
    ext.extract_info_diary(prefix='raw')

    logging.info('[# 2 -------- TRANSFORMANDO DADOS----------#]')
    time.sleep(5)

    tr = Transform(spark)
     
    # busca os dados do bucket raw do s3 e le e concatena em csv
    df_raw = tr.read_s3_files(prefix='raw')      
    
    # recebe o df lido em csv e faz o tratamento        
    df_transformed = tr.transform_data(df_raw) 
    
    # recebe o df que foi tratado e faz calculos            
    df_metricas = tr.calculate_metricas(df_transformed)   

    # Salva fundos
    tr.upload_stage({lista_tabelas[0]: df_transformed,
                    lista_tabelas[1]: df_metricas})

    logging.info('[# 3 -------- CARREGANDO DADOS NO DATA WAREHOUSE ----------#]')
    time.sleep(5)

    lista_tabelas = ['metricas','fundos']

    load = LoadDw(spark,
                host="postgres",
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD)

    load.create_table(filepath='/sql/create_tables.sql')
    load.insert_data(schema='cvm_teste', tables=lista_tabelas)

    load.clean_temp_folder()

    logging.info('[#################### PIPELINE FINALIZADO #################]')
    time.sleep(2)

    spark.stop()

if __name__ == "__main__":
    main()

