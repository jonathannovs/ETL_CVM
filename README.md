
# para rodaar o container
$ docker compose up -d

# para rodar o job spark
 executar assim q conttruir para criar pasta home
$ docker exec -e HOME=/root -it spark-master spark-submit --master spark://spark-master:7077 /app/main.py


# entrar no container spark

### entrar dentro do container spark instalar pacotes voltar para o docker e rodar o main.py

$ docker exec -it spark-master bash 
pip install -r /app/requirements.txt  #instalar pacotes
$ python /app/main.py
ou
$ docker exec -it spark-master spark-submit /app/main.py

# acessar spark ui
http://localhost:8080



# para rodar com job spark 
$ docker exec -it spark-master spark-submit /app/main.py  $

# para rodar sem spark, nao precisando entrar no conteiner
docker exec -it spark-master python /app/main.py


# acessar o jupyter
$ docker logs jupyter_pyspark


