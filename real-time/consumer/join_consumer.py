# docker cp join_consumer.py spark-master:join_consumer.py
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 join_consumer.py
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

stream_src = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "network-data-src") \
    .load()

stream_dst = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "network-data-dst") \
    .option("startingOffsets", "earliest") \
    .load()

schema_src = StructType([
    StructField("src_ip", StringType(), True),
    StructField("src_port", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("length", StringType(), True),
    
])

schema_dst = StructType([
    StructField("dst_ip", StringType(), True),
    StructField("dst_port", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("length", StringType(), True),
    
])

stream_dst = stream_dst.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_dst).alias("data")) \
    .select("data.*")


stream_src = stream_src.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_src).alias("data")) \
    .select("data.*")


#upit 5 - spajanje po src i dst
join_streams = stream_src.join(stream_dst, stream_dst.dst_ip == stream_src.src_ip , "inner")

join_streams = join_streams.withColumn("join_timestamp", current_timestamp())


query = join_streams \
    .writeStream \
    .outputMode("append") \
    .option("truncate", "false") \
    .trigger(processingTime='10 seconds') \
    .format("console") \
    .start()


query.awaitTermination()
