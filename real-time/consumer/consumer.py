# docker cp consumer.py spark-master:consumer.py
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession.builder \
    .appName("SSS - Kafka String Consumer") \
    .getOrCreate()

quiet_logs(spark)

rez = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "network-data") \
    .option("startingOffsets", "earliest") \
    .load()

rez = rez.selectExpr("CAST(value AS STRING) as value")

query = rez \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .format("console") \
    .option("truncate", "false") \
    .start()


query.awaitTermination()
