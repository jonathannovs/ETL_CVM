import sys
sys.path.append("/src")  

# from transform import Transform
from extract import ExtractCvm

def main():
    # transformer = Transform()
    # transformer.run()
    # transformer.stop()

    extractor = ExtractCvm(start_date=2022, bucket_name="s3-cvm-fii")
    extractor.create_bucket()
    extractor.extract_info_diary()


if __name__ == "__main__":
    main()
