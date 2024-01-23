#!/usr/bin/python
### before spark-submit: export PYTHONIOENCODING=utf8

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("uni").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)


schemaString = "flow_key src_ip_numeric src_ip src_port dst_ip dst_port proto pktTotalCount \
    octetTotalCount min_ps max_ps avg_ps std_dev_ps flowStart flowEnd flowDuration min_piat \
    max_piat avg_piat std_dev_piat f_pktTotalCount f_octetTotalCount f_min_ps f_max_ps f_avg_ps \
    f_std_dev_ps f_flowStart f_flowEnd f_flowDuration f_min_piat f_max_piat f_avg_piat \
    f_std_dev_piat b_pktTotalCount b_octetTotalCount b_min_ps b_max_ps b_avg_ps b_std_dev_ps\
    b_flowStart b_flowEnd b_flowDuration b_min_piat b_max_piat b_avg_piat b_std_dev_piat flowEndReason \
    category application_protocol web_service"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = spark.read.csv("hdfs://namenode:9000/data/data.csv", header=True, mode="DROPMALFORMED", schema=schema)

# df.printSchema()

df = df.withColumn("max_ps", df["max_ps"].cast(IntegerType()))
df = df.withColumn("min_ps", df["min_ps"].cast(IntegerType()))
df = df.withColumn("max_piat", df["max_ps"].cast(FloatType()))
df = df.withColumn("min_piat", df["min_ps"].cast(FloatType()))
df = df.withColumn("flowDuration", df["flowDuration"].cast(FloatType()))
df.createOrReplaceTempView("ndf")

queryMinMaxPacketSize = "SELECT src_ip, AVG(max_ps) as avg_max_packet_size, AVG(min_ps) as avg_min_packet_size \
         FROM ndf \
         WHERE  application_protocol = 'HTTP'\
         GROUP BY src_ip"
sqlDF1 = spark.sql(queryMinMaxPacketSize)
sqlDF1.show(20, False)

sqlDF1.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/results/AvgMinMaxPacketSize.csv", header = 'true')

queryMinMaxInterArrTime = "SELECT dst_ip, AVG(max_piat) as avg_max_interarrival_time, AVG(min_piat) as avg_min_pinterarrival_time \
         FROM ndf \
         WHERE  flowDuration > 1 \
         GROUP BY dst_ip"
sqlDF2 = spark.sql(queryMinMaxInterArrTime)
sqlDF2.show(20, False)

sqlDF2.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/results/AvgMinMaxInterArrTime.csv", header = 'true')


