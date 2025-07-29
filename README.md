
# para rodaar o container
$ docker compose up -d

# para rodar o job spark
$ docker exec -e HOME=/root -it spark-master spark-submit --master spark://spark-master:7077 /app/main.py

# ou

# entrar no container spark

# instalar pacotes
$ pip install -r /app/requirements.txt  #instalar pacotes

# entrar no container
$ docker exec -it spark-master bash 

# executar o scrip dentro do container spark
$ python /app/main.py

# acessar spark ui
http://localhost:8080

# para rodar com job spark 
$ docker exec -it spark-master spark-submit /app/main.py  $