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


    logging.info('[# 1 -------- EXTRAINDO CVM ----------#]')
    time.sleep(5)

    ext = ExtractCvm(start_date=2022, bucket_name="s3-cvm-fii")
    ext.create_bucket()
    ext.extract_info_diary(prefix='raw')

    logging.info('[# 2 -------- TRANSFORMANDO DADOS----------#]')
    time.sleep(5)

    trans = Transform(spark)
    df_raw = trans.read_s3_files(prefix='raw')
    df_transformed = trans.transform_data(df_raw)
    #teste = trans.transform_teste(df_transformed) 
    trans.upload_stage(df_transformed, prefix="stage/full")
    
    logging.info('[# 3 -------- CARREGANDO DADOS NO DATA WAREHOUSE ----------#]')
    time.sleep(5)

    load = LoadDw(spark, prefix = 'stage/full', 
                host="postgres",
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD)
    load.consulta_bucket()
    #load.delete_files('s3-cvm-fii','stage/2025')
    load.create_table(filepath='/sql/create_tables.sql')
    load.insert_data()

    logging.info('[#################### PIPELINE FINALIZADO #################]')
    time.sleep(2)

    spark.stop()

if __name__ == "__main__":
    main()

