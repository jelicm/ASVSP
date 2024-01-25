# docker cp consumer.py spark-master:consumer.py
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

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


schema = StructType([
    StructField("src_ip", StringType(), True),
    StructField("dst_ip", StringType(), True),
    StructField("src_port", StringType(), True),
    StructField("dst_port", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("length", StringType(), True),
    
])

rez = rez.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

rez = rez.withColumn("received_timestamp", current_timestamp())

#upit 1 -koliko razl src_ip je bilo u prethodnih 10 sekundi za tcp protokol
query1_df = rez \
    .filter(col("protocol") == "TCP") \
    .groupBy(window(col("received_timestamp"), "10 seconds")) \
    .agg(count("src_ip").alias("different_src_ip")) \
    .select("window.start", "window.end", "different_src_ip")

#upit 2 - ukupna veličina paketa pristiglih u poslednjih 10ak sekundi
query2_df = rez \
    .filter(col("protocol")=="TCP") \
    .groupBy(window(col("received_timestamp"), "10 seconds")) \
    .agg(sum("length").alias("sum_length")) \
    .select("window.start", "window.end", "sum_length")


#upit 3 -
query3_df = rez \
    .groupBy(window(col("received_timestamp"), "15 seconds")) \
    .agg(count("protocol").alias("different_protocols")) \
    .select("window.start", "window.end", "different_protocols")

# upit 4 - nije gotov
#query4_df = rez \
#    .groupBy(window(col("received_timestamp"), "15 seconds").alias("tw")) \
#    .agg(count("dst_ip").alias("different_ips")) \
    


#windowQuery4 = Window.partitionBy("start", "end").orderBy("different_ips")
#query4_df = query4_df.withColumn("row_num", row_number().over(windowQuery4))
#query4_df = query4_df.filter(col("row_num") == 1)


query = query2_df \
    .writeStream \
    .outputMode("complete") \
    .trigger(processingTime='10 seconds') \
    .format("console") \
    .start()

#.option("truncate", "false") \
query.awaitTermination()
