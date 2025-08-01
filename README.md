
# para rodaar o container
$ docker compose up -d

# para rodar o job spark
 executar assim q conttruir para criar pasta home
# $ docker exec -e HOME=/root -it spark-master spark-submit --master spark://spark-master:7077 /app/main.py


# entrar no container spark

### entrar dentro do container spark instalar pacotes voltar para o docker e rodar o main.py

$ docker exec -it spark-master bash 
pip install -r /app/requirements.txt  #instalar pacotes
$ python /app/main.py
ou
$ docker exec -it spark-master spark-submit /app/main.py

# acessar spark ui
http://localhost:8080


# para rodar sem spark, nao precisando entrar no conteiner
docker exec -it spark-master python /app/main.py


# acessar o jupyter
$ docker logs jupyter_pyspark


import socket
import time
from pyspark.sql import SparkSession

def wait_for_spark_master(host="spark-master", port=7077, timeout=60):
    print(f"Esperando conexão com spark://{host}:{port}")
    start = time.time()
    while time.time() - start < timeout:
        try:
            socket.create_connection((host, port), timeout=2)
            print("✅ Spark Master está disponível.")
            return
        except OSError:
            print("⏳ Aguardando Spark Master...")
            time.sleep(2)
    raise TimeoutError("❌ Timeout esperando Spark Master.")

# Aguarda o master
wait_for_spark_master()

# Cria SparkSession
spark = SparkSession.builder \
    .appName("Notebook PySpark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("SparkSession criada com sucesso("✅ ")



