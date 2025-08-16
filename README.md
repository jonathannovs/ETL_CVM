
Este projeto ELT  extraí dados de fundos de investimento da comissão de valores mobiliarios















# para executar o projeto

## 1. Subir o container
```bash
 docker compose up -d
```

## 2. Subir o container

```bash
docker exec -it spark-master pip install -r /app/requirements.txt
```

## 3.Executar main.py
```bash
docker exec -it spark-master spark-submit \
  --jars /opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar \
  --driver-class-path /opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar \
  --master spark://spark-master:7077 /app/main.py
```