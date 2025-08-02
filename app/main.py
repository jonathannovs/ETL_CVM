import sys
sys.path.append("/src")  

from transform import Transform
from extract import ExtractCvm
from load import LoadDw

def main():

    # e = ExtractCvm(start_date=2022, end_date='2025-07-31', bucket_name="s3-cvm-fii")
    # e.create_bucket()
    # e.extract_info_diary()

    # t = Transform()
    # df_raw = t.read_s3_files()
    # df_transformed = t.transform_data(df_raw)
    # #teste = t.transform_teste(df_raw)
    # t.upload_stage(df_transformed, prefix="stage")

    l = LoadDw()
    l.consulta_bucket()

if __name__ == "__main__":
    main()
