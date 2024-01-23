from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col, desc, row_number
import os

HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("uni").setMaster("spark://spark-master:7077") 
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

quiet_logs(spark)

hive_table_name = "cleaned_data"

df = spark.table(hive_table_name)

df.createOrReplaceTempView("ndf")

#upit 1
queryMinMaxPacketSize = "SELECT src_ip, AVG(max_ps) as avg_max_packet_size, AVG(min_ps) as avg_min_packet_size \
         FROM ndf \
         WHERE  application_protocol = 'HTTP'\
         GROUP BY src_ip"
sqlDF1 = spark.sql(queryMinMaxPacketSize)
sqlDF1.show(10, False)


#upit 2

queryMinMaxInterArrTime = "SELECT dst_ip, AVG(max_piat) as avg_max_interarrival_time, AVG(min_piat) as avg_min_pinterarrival_time \
         FROM ndf \
         WHERE  flowDuration > 1 \
         GROUP BY dst_ip"
sqlDF2 = spark.sql(queryMinMaxInterArrTime)
sqlDF2.show(10, False)

#upit 3

windowSpec = Window.partitionBy("proto").orderBy((col("flowDuration") / col("pktTotalCount")).desc())
df_query3 = df.withColumn("dense_rank", dense_rank().over(windowSpec))

result_df = df_query3.filter(col("dense_rank") == 1)

result_df.show()

#upit 4

#upit 5

#upit 6

#upit 7

#upit 8

windowQuery8 = Window.partitionBy("flowEndReason", "src_ip", "dst_ip").orderBy((col("flowDuration") / col("pktTotalCount")).desc())
df_query8 = df.withColumn("row_number", row_number().over(windowQuery8))

result_df = df_query8.filter((col("row_number") == 1) & (col("flowEndReason") == 2))

result_df.show()

#upit 9

#upit 10

spark.stop()
