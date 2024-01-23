from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
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

hive_table_name = "clean_data"

df = spark.table(hive_table_name)

df.createOrReplaceTempView("ndf")

queryMinMaxPacketSize = "SELECT src_ip, AVG(max_ps) as avg_max_packet_size, AVG(min_ps) as avg_min_packet_size \
         FROM ndf \
         WHERE  application_protocol = 'HTTP'\
         GROUP BY src_ip"
sqlDF1 = spark.sql(queryMinMaxPacketSize)
sqlDF1.show(10, False)


queryMinMaxInterArrTime = "SELECT dst_ip, AVG(max_piat) as avg_max_interarrival_time, AVG(min_piat) as avg_min_pinterarrival_time \
         FROM ndf \
         WHERE  flowDuration > 1 \
         GROUP BY dst_ip"
sqlDF2 = spark.sql(queryMinMaxInterArrTime)
sqlDF2.show(10, False)


spark.stop()
