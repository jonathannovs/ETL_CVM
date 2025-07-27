# src/transform.py

from pyspark.sql import SparkSession

class Transform:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Teste PySpark") \
            .master("spark://spark-master:7077") \
            .getOrCreate()

    def run(self):
        data = [("Alice", 25), ("Bob", 30), ("Carol", 27)]
        df = self.spark.createDataFrame(data, ["Nome", "Idade"])
        df.show()

    def stop(self):
        self.spark.stop()
