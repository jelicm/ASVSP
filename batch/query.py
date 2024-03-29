from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col, desc, row_number, count, max, min, avg, sum, split
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

sqlDF1.write.format("jdbc").option("url", "jdbc:postgresql://pg:5432/postgres").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "query1").\
    option("user", "postgres").\
    option("password", "postgres").\
    mode("overwrite").save()


#upit 2

queryMinMaxInterArrTime = "SELECT dst_ip, AVG(max_piat) as avg_max_interarrival_time, AVG(min_piat) as avg_min_pinterarrival_time \
         FROM ndf \
         WHERE  flowDuration > 1 \
         GROUP BY dst_ip"
sqlDF2 = spark.sql(queryMinMaxInterArrTime)
sqlDF2.show(10, False)

sqlDF2.write.format("jdbc").option("url", "jdbc:postgresql://pg:5432/postgres").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "query2").\
    option("user", "postgres").\
    option("password", "postgres").\
    mode("overwrite").save()

#upit 3

windowSpec = Window.partitionBy("proto").orderBy((col("flowDuration") / col("pktTotalCount")).desc())
df_query3 = df.withColumn("dense_rank", dense_rank().over(windowSpec))

df_query3 = df_query3.filter(col("dense_rank") == 1) \
                     .select("proto", "src_ip", "dst_ip", "flowDuration", "pktTotalCount")

df_query3.show()

df_query3.write.format("jdbc").option("url", "jdbc:postgresql://pg:5432/postgres").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "query3").\
    option("user", "postgres").\
    option("password", "postgres").\
    mode("overwrite").save()

#upit 4

windowQuery4 = Window.partitionBy("start_year", "start_month", "start_day_of_month", "flowEndReason")
df_query4 = df.withColumn("flow_count", count("flow_key").over(windowQuery4))

windowQuery4 = Window.partitionBy("start_year", "start_month", "start_day_of_month").orderBy(col("flow_count").desc())
df_query4 = df_query4.withColumn("row_num", row_number().over(windowQuery4))

df_query4 = df_query4.filter(col("row_num") == 1) \
                     .select("start_year", "start_month", "start_day_of_month", "flowEndReason")

df_query4.show()

df_query4.write.format("jdbc").option("url", "jdbc:postgresql://pg:5432/postgres").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "query4").\
    option("user", "postgres").\
    option("password", "postgres").\
    mode("overwrite").save()

#upit 5
windowQuery5 = Window.partitionBy("start_hour_of_day")
df_query5 = df.withColumn("avg_min_piat", avg("min_piat").over(windowQuery5))
df_query5 = df_query5.withColumn("min_avg_ps", min("avg_ps").over(windowQuery5))

df_query5 = df_query5.filter(col("min_avg_ps") > 30) \
                     .orderBy("avg_min_piat")\
                     .select("start_hour_of_day", "avg_min_piat")

df_query5.show(1, False)

df_query5.write.format("jdbc").option("url", "jdbc:postgresql://pg:5432/postgres").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "query5").\
    option("user", "postgres").\
    option("password", "postgres").\
    mode("overwrite").save()

#upit 6
df_query6 = df.filter(col("start_month")=="04")
df_query6 = df_query6.filter(col("flowDuration").isNotNull())
windowQuery6 = Window.partitionBy("start_hour_of_day", "src_ip", "src_port")
df_query6 = df_query6.withColumn("sum_flow", sum("flowDuration").over(windowQuery6))
windowQuery6 = Window.partitionBy("start_hour_of_day", "src_ip", "src_port").orderBy(col("sum_flow").desc())

df_query6 = df_query6.withColumn("row_num", row_number().over(windowQuery6))
df_query6 = df_query6.filter(col("row_num") == 1) \
                     .select("start_hour_of_day", "src_ip", "src_port", "sum_flow")  

df_query6.show()

df_query6.write.format("jdbc").option("url", "jdbc:postgresql://pg:5432/postgres").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "query6").\
    option("user", "postgres").\
    option("password", "postgres").\
    mode("overwrite").save()

#upit 7

windowQuery7 = Window.partitionBy("proto", "src_ip", "start_year", "start_month", "start_day_of_month")
df_query7 = df.withColumn("sum_max_ps", sum("max_ps").over(windowQuery7))
windowQuery7 = Window.partitionBy("src_ip", "start_year", "start_month", "start_day_of_month").orderBy(col("sum_max_ps").desc())
df_query7 = df_query7.withColumn("row_num", row_number().over(windowQuery7))

df_query7 = df_query7.filter(col("row_num") == 1)\
                      .select("proto", "src_ip", "start_year", "start_month", "start_day_of_month")
df_query7.show()

df_query7.write.format("jdbc").option("url", "jdbc:postgresql://pg:5432/postgres").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "query7").\
    option("user", "postgres").\
    option("password", "postgres").\
    mode("overwrite").save()

#upit 8

windowQuery8 = Window.partitionBy("flowEndReason", "src_ip", "dst_ip").orderBy((col("flowDuration") / col("pktTotalCount")).desc())
df_query8 = df.withColumn("row_number", row_number().over(windowQuery8))

df_query8 = df_query8.filter((col("row_number") == 1) & (col("flowEndReason") == 2)) \
                      .select("flowDuration", "pktTotalCount", "src_ip", "dst_ip", "flowEndReason")

df_query8.show()

df_query8.write.format("jdbc").option("url", "jdbc:postgresql://pg:5432/postgres").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "query8").\
    option("user", "postgres").\
    option("password", "postgres").\
    mode("overwrite").save()

#upit 9

df_query9 = df.filter((col("start_month")=="04") & (col("proto")==6) & (col("octetTotalCount")/col("pktTotalCount") > 45))
windowQuery9 = Window.partitionBy("web_service")
df_query9 = df_query9.withColumn("sum_bytes", sum("octetTotalCount").over(windowQuery9)) \
                      .orderBy("sum_bytes").select("web_service")
df_query9.show(1, False)

df_query9.write.format("jdbc").option("url", "jdbc:postgresql://pg:5432/postgres").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "query9").\
    option("user", "postgres").\
    option("password", "postgres").\
    mode("overwrite").save()

#upit 10
df_query10 = df.filter(col("proto")==17)
df_query10 = df_query10.withColumn("last_octet", split(col("src_ip"), "\.")[3].cast("int"))
df_query10 = df_query10.filter(col("last_octet")>17)
windowQuery10 = Window.partitionBy("src_ip").orderBy(col("pktTotalCount").desc())

df_query10 = df_query10.withColumn("row_num", row_number().over(windowQuery10))
df_query10 = df_query10.filter(col("row_num") == 1) \
                      .select("proto", "src_ip", "last_octet")
df_query10.show()

df_query10.write.format("jdbc").option("url", "jdbc:postgresql://pg:5432/postgres").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "query10").\
    option("user", "postgres").\
    option("password", "postgres").\
    mode("overwrite").save()
                                                    

spark.stop()
