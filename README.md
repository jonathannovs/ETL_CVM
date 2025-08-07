
# para rodaar o container
$ docker compose up -d

# para rodar o job spark
 executar assim q conttruir para criar pasta home
# $ docker exec -e HOME=/root -it spark-master spark-submit --master spark://spark-master:7077 /app/main.py

# entrar dentro do container spark instalar pacotes voltar para o docker e rodar o main.py

$ docker exec -it spark-master pip install -r /app/requirements.txt
$ docker exec -it spark-master spark-submit /app/main.py

# acessar spark ui
http://localhost:8080


-- para rodar sem spark
docker exec -it spark-master python /app/main.py


# acessar o jupyter
$ docker logs jupyter_pyspark

# entrar no conteiner: 
$ docker exec -it spark-master bash 
rodar: $ python /app/main.py

# rodando pipeline todo
 docker exec -it spark-master spark-submit --jars /opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar --driver-class-path /opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar --master spark://spark-master:7077 /app/main.py