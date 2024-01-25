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

def save_to_postgres(result, tablename):
    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://pg:5432/postgres").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", tablename).\
        option("user", "postgres").\
        option("password", "postgres").\
        mode("append").save()
    print("saved to db!")

spark = SparkSession.builder \
    .appName("SSS - Kafka String Consumer") \
    .getOrCreate()

quiet_logs(spark)

rez = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "network-data-src") \
    .load()


schema = StructType([
    StructField("src_ip", StringType(), True),
    StructField("src_port", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("length", StringType(), True),
    
])

rez = rez.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

rez = rez.withColumn("received_timestamp", current_timestamp())

#upit 1 -koliko razl src_ip je bilo u prethodnih 10 sekundi za određeni protokol
query1_df = rez \
    .groupBy(col("protocol"),window(col("received_timestamp"), "10 seconds")) \
    .agg(count("src_ip").alias("different_src_ip")) \
    .select("window.start", "window.end", "different_src_ip", "protocol")

"""
#upit 2 - ukupna veličina paketa pristiglih u poslednjih 10 sekundi za TCP protokol
query2_df = rez \
    .filter(col("protocol")=="TCP") \
    .groupBy(window(col("received_timestamp"), "10 seconds")) \
    .agg(sum("length").alias("sum_length")) \
    .select("window.start", "window.end", "sum_length")


#upit 3 - broj različitih protokola u prethodnih 15 sekundi
query3_df = rez \
    .groupBy(window(col("received_timestamp"), "15 seconds")) \
    .agg(count("protocol").alias("different_protocols")) \
    .select("window.start", "window.end", "different_protocols")


#upit 4 -  standardna devijacija veličine paketa za određeni port
query4_df = rez \
    .groupBy(window(col("received_timestamp"), "15 seconds"), "src_port") \
    .agg(stddev("length").alias("stddev_length")) \
    .select("window.start", "window.end", "stddev_length")
"""

query1 = query1_df.writeStream.outputMode("update") \
        .foreachBatch(lambda df, iter: save_to_postgres(df, "stream_query1"))\
        .start()


query1.awaitTermination()


