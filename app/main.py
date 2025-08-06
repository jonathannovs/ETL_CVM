import sys
import logging 
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sys.path.append("/src")  


from transform import Transform
from extract import ExtractCvm
from load import LoadDw

DB_NAME = 'CVM'
DB_USER = 'JONANOV'
DB_PASSWORD = 'admin'

def main():

#     logging.info('[# 1 -------- EXTRAINDO CVM ----------#]')
#     ext = ExtractCvm(start_date=2022, bucket_name="s3-cvm-fii")
#     ext.create_bucket()
#     ext.extract_info_diary(prefix='raw')

#     logging.info('[# 2 -------- TRANSFORMANDO DADOS----------#]')
#     trans = Transform()
#     df_raw = trans.read_s3_files(prefix='raw')
#     df_transformed = trans.transform_data(df_raw)
#     teste = trans.transform_teste(df_transformed) 
#     trans.upload_stage(teste, prefix="teste")

#     logging.info('\n[# 3 -------- CONSULTANDO S3 ----------#]')
# #
    load = LoadDw(prefix = 'teste', 
                host="postgres",
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD)
    load.consulta_bucket()
    time.sleep(5)
    load.delete_files(path='s3a://s3-cvm-fii/teste')
    # load.create_table(filepath='/sql/create_tables.sql')
    # load.insert_data()

    logging.info('\n[#################### PIPELINE FINALIZADO #################]')

if __name__ == "__main__":
    main()

