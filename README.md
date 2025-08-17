

# Projeto de Engenharia de Dados: ETL de Fundos de Investimento da CVM com Spark e AWS simulado.

Este projeto é um pipeline de dados completo para extrair, transformar e carregar (ETL) os informes diários de fundos de investimento brasileiros, disponibilizados publicamente pela CVM.

## 📜 Sobre o Projeto
O projeto nasceu da união de duas áreas de grande interesse: o mercado financeiro e a engenharia de dados. O objetivo principal foi construir um pipeline de dados robusto e escalável para processar um grande volume de informações diárias dos fundos de investimento brasileiros ao longo do período de 2022 até os dias atuais, disponibilizadas pela Comissão de Valores Mobiliários (CVM).

O pipeline automatiza todo o processo, desde o download dos dados brutos até a disponibilização em um banco de dados relacional, pronto para análises e visualizações.

## 🏗️ Arquitetura da Solução
A solução foi desenhada em um fluxo de ETL (Extração, Transformação e Carregamento) e totalmente conteinerizada com Docker para garantir portabilidade e um ambiente de desenvolvimento isolado.

### 1. Extração (Extraction)
Fonte de Dados: Informes diários da CVM (valor da cota, Patrimônio Líquido, aplicações e resgates) a partir de 2022.

Tecnologia: Scripts em Python utilizando as bibliotecas requests para o download dos arquivos e boto3 para a comunicação com o S3.

Armazenamento Bruto (Raw): Os dados são salvos em um bucket no Amazon S3, que é simulado localmente através do LocalStack.

### 2. Transformação (Transformation)
Desafio: Processar um grande volume de dados, que ultrapassa 22 milhões de registros.

Framework: Apache Spark (via PySpark) foi escolhido por sua capacidade de processamento distribuído e alta performance.

Limpeza e padronização dos dados.

Tratamento de valores nulos e tipos de dados.

Enriquecimento com novas métricas, como o cálculo de PnL (Profit and Loss) e variação diária da cota.

### 3. Carregamento (Loading)
Destino: Os dados limpos e processados são carregados em um banco de dados PostgreSQL.

Propósito: Disponibilizar os dados estruturados para consumo por ferramentas de BI, dashboards ou outras aplicações analíticas.

## 🛠️ Tecnologias Utilizadas

-  Python:	Linguagem principal para scripts de extração e transformação.
-  Apache Spark:	Framework de processamento de dados distribuído para a etapa de transformação.
-  PostgreSQL:	Banco de dados relacional para armazenar os dados tratados.
-  Amazon S3 (LocalStack):	Armazenamento dos dados brutos (Data Lake).
-  Docker & Docker Compose:	Conteinerização e orquestração de toda a infraestrutura do projeto.


## 💻 Estrutura de Pastas

```bash
ETL_CVM/
├── app/                 # Scripts principais do projeto
├── jars/                # Bibliotecas e drivers Java para Spark
├── notebooks/           # Notebooks para análise e testes
├── sql/                 # Scripts SQL para banco de dados
├── src/                 # Código fonte do ETL
├── docker-compose.yml   # Orquestração Docker
├── Arquitetura ETL.png  # Diagrama da arquitetura ETL
├── README.md            # Documentação do projeto
└── .gitignore           # Arquivos ignorados pelo Git
```

## 🚀 Como Executar o Projeto
Siga os passos abaixo para executar todo o pipeline de ETL localmente.

Pré-requisitos
Antes de começar, garanta que você tenha as seguintes ferramentas instaladas:

- Git
- Docker

Passo a Passo
1. Clone o Repositório


```bash
git clone https://github.com/jonathannovs/ETL_CVM.git
cd ETL_CVM
```

2. Inicie os Contêineres
Este comando irá construir e iniciar todos os serviços definidos no docker-compose.yml (Spark, PostgreSQL, etc.) em segundo plano.
```bash
docker-compose up -d
```

3. Instale as Dependências Python
Acesse o contêiner do Spark Master para instalar as bibliotecas Python necessárias.
```bash
docker exec -it spark-master pip install -r /app/requirements.txt
```

4. Execute o Pipeline ETL
Execute o script principal main.py usando spark-submit. Este comando submete a aplicação para o cluster Spark.
```bash
docker exec -it spark-master spark-submit \
  --jars /opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar \
  --driver-class-path /opt/bitnami/spark/jars-custom/postgresql-42.6.0.jar \
  --master spark://spark-master:7077 \
  /app/main.py
```

O pipeline começará a extrair, transformar e carregar os dados. O processo pode levar alguns minutos dependendo do volume de dados e da performance da sua máquina.

5. Verifique os Dados no PostgreSQL
Após a execução do pipeline, você pode se conectar ao banco de dados para verificar se os dados foram carregados corretamente.
```bash SQL
SELECT * FROM cvm.fundos LIMIT 10;
```

## 🔮 Próximos Passos
O roadmap futuro deste projeto inclui a migração da solução local para uma arquitetura 100% serverless na nuvem AWS, visando maior escalabilidade, resiliência e automação.

[ ] Data Lake: Utilizar o Amazon S3 como Data Lake definitivo para os dados brutos e processados.

[ ] Processamento: Substituir o cluster Spark local por AWS Glue ou Amazon EMR para processamento distribuído sob demanda.

[ ] Data Warehouse: Migrar o PostgreSQL para o Amazon Redshift como Data Warehouse (OLAP) para otimizar consultas analíticas complexas e de alta performance.

[ ] Orquestração: Implementar o AWS Step Functions ou Apache Airflow (MWAA) para orquestrar e agendar o pipeline.