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


hdfs_csv_path = "hdfs://namenode:9000/data/data.csv"

# Read CSV into a DataFrame
df = spark.read.csv(hdfs_csv_path, header=True, inferSchema=True)

# List of columns to drop
columns_to_drop = [ "std_dev_piat", "f_pktTotalCount",
                    "f_octetTotalCount", "f_min_ps", "f_max_ps", "f_avg_ps", "f_std_dev_ps",
                    "f_flowStart", "f_flowEnd", "f_flowDuration", "f_min_piat", "f_max_piat",
                    "f_avg_piat", "f_std_dev_piat", "b_pktTotalCount", "b_octetTotalCount",
                    "b_min_ps", "b_max_ps", "b_avg_ps", "b_std_dev_ps", "b_flowStart","b_flowEnd", 
                    "b_flowDuration", "b_min_piat", "b_max_piat", "b_avg_piat", "b_std_dev_piat"]


df = df.drop(*columns_to_drop)

df.printSchema()


spark.sql("DROP TABLE IF EXISTS cleaned_data")
df.write.mode("overwrite").saveAsTable("cleaned_data")


print("Preprocessing is finished and saved to table!")
spark.stop()
