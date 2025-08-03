import sys
import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sys.path.append("/src")  


from transform import Transform
from extract import ExtractCvm
from load import LoadDw

def main():

    logging.info('[# 1 -------- EXTRAINDO CVM ----------#]')
    ext = ExtractCvm(start_date=2022, end_date='2025-07-31', bucket_name="s3-cvm-fii")
    ext.create_bucket()
    ext.extract_info_diary()

    logging.info('[# 2 -------- TRANSFORMANDO DADOS----------#]')
    trans = Transform()
    df_raw = trans.read_s3_files()
    df_transformed = trans.transform_data(df_raw)
    teste = trans.transform_teste(df_transformed) 
    trans.upload_stage(teste, prefix="stage-test2")

    logging.info('[# 3 -------- CONSULTANDO S3 ----------#]')
    l = LoadDw()
    l.consulta_bucket(prefix="stage-test2")

    logging.info('[#################### PIPELINE FINALIZADO #################]')

if __name__ == "__main__":
    main()

