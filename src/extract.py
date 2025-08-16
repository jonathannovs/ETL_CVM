import requests
import pandas as pd
import zipfile
import os
from io import BytesIO, StringIO
import logging
from datetime import datetime
from tqdm import tqdm
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ExtractCvm:

    def __init__(self,bucket_name:str, start_date= '2022-01-01', end_date = None):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.start_year = self.start_date.year 
        self.start_month = self.start_date.month
        self.end_date = datetime.today().date() if end_date is None else datetime.strptime(end_date, "%Y-%m-%d")
        self.end_year = self.end_date.year
        self.end_month = self.end_date.month
        self.bucket_name = bucket_name
        self.s3 = boto3.client(
            "s3",
            endpoint_url="http://localstack:4566",  
            aws_access_key_id="test",            
            aws_secret_access_key="test",
            region_name="us-east-1")
        
    def create_bucket(self):
        try:
            self.s3.create_bucket(Bucket=self.bucket_name)
            logging.info(f"Bucket '{self.bucket_name}' criado com sucesso.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                logging.exception(f"Bucket '{self.bucket_name}' já existe.")
            else:
                logging.exception(f"Erro ao criar o bucket: {e}")

    def extract_info_diary(self,prefix):
        periodos = []
        for year in range(self.start_year, self.end_year + 1):
            start_month = self.start_month if year == self.start_year else 1
            end_month = self.end_month if year == self.end_year else 12
            for month in range(start_month, end_month +1):
                periodos.append((year, month))

        for year, month in tqdm(periodos, desc="Baixando relatórios", unit=" file"):
            yyyymm = f"{year}{month:02d}"
            file_name = f"inf_diario_fi_{yyyymm}.csv"
            prefix=prefix
            s3_key = f"{prefix}/{file_name}"
            url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{yyyymm}.zip"

            try:
                self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
                logging.info(f"O arquivo '{s3_key}' já existe no bucket. Pulando download.")
                continue  # pula para o próximo mês
            except ClientError as e:
                if e.response['Error']['Code'] != "404":
                    logging.exception(f"Erro ao verificar existência do arquivo '{s3_key}': {e}")
                    continue  
            try:
                response = requests.get(url)
                response.raise_for_status()
                zip_file_in_memory = BytesIO(response.content)

                with zipfile.ZipFile(zip_file_in_memory) as zf:
                    with zf.open(file_name) as file:
                        self.s3.upload_fileobj(file, self.bucket_name, s3_key)

                    logging.info(f"Arquivo '{file_name}' ({len(response.content):,} bytes) enviado para o S3 no caminho '{s3_key}'.")

            except requests.exceptions.HTTPError as err:
                logging.exception(f"Erro HTTP: O arquivo para {month:02d}/{year} não foi encontrado ou o servidor retornou um erro.")

            except Exception as err:
                logging.exception(f"Erro inesperado em {month:02d}/{year}: {err}")


    def extract_infos_funds(self, prefix):
        for ano in range(self.start_year, self.end_year +1):

            file_name = f"infos_fundos{ano}.csv"
            s3_key = f"{prefix}/{file_name}"

            try:
                self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
                logging.info(f"O arquivo '{s3_key}' já existe no bucket. Pulando download.")
                continue 
            except ClientError as e:
                if e.response['Error']['Code'] != "404":
                    logging.exception(f"Erro ao verificar existência do arquivo '{s3_key}': {e}")
                    continue  
            url = f'https://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/DADOS/extrato_fi_{ano}.csv'
            response = requests.get(url)
            response.raise_for_status()
            file = BytesIO(response.content)

            self.s3.upload_fileobj(file, self.bucket_name, s3_key)
            logging.info(f"Arquivo '{file_name}' ({len(response.content):,} bytes)  enviado para o S3 no caminho '{s3_key}'.")