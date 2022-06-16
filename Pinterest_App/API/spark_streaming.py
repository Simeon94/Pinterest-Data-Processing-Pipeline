from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType, MapType
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os

data_spark_schema = ArrayType(StructType([
        StructField("category", StringType(), True), \
        StructField("index", StringType(), True), \
        StructField("unique_id", StringType(), True), \
        StructField("title", StringType(), True), \
        StructField("description", StringType(), True), \
        StructField("follower_count", IntegerType(), True), \
        StructField("tag_list", StringType(), True), \
        StructField("is_image_or_video", StringType(), True), \
        StructField("image_src", StringType(), True), \
        StructField("downloaded", StringType(), True), \
        StructField("save_location",StringType(), True)]))
#sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.6 pyspark-shell'
#spark = SparkSession.builder.config("spark.driver.extraClassPath", sparkClassPath).appName("Kafka").getOrCreate()

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.2.6 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'

kafka_topic_name = "MyFirstKafkaTopic"
kafka_bootstrap_servers = "localhost:9092"

#spark = SparkSession.builder.appName("Kafka").getOrCreate()

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL") \
    .config("spark.jars", "/home/ubuntu/postgresql-42.2.6.jar") \
    .getOrCreate()

#sc = spark.sparkContext

streaming_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
            .option("startingOffsets", "latest") \
                .load()
                    #.selectExpr("CAST(value AS STRING)")

#streaming_df1 = streaming_df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# 
def clean_data(dataframe, epoch_id):
    dataframe = dataframe.selectExpr("CAST(value AS STRING)")

    dataframe = dataframe.withColumn("temp", F.explode(F.from_json("value", data_spark_schema))).select("temp.*")
    


streaming_df.writeStream.outputMode("append").foreachBatch(clean_data).format("console").option("truncate", False).start().awaitTermination()

# streaming_df1.show(truncate=True)

# streaming_df1.writeStream.outputMode("append").format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/pinterest") \
#         .option("driver", "org.postgresql.Driver") \
#             .option("dbtable", "pinterest") \
#                 .option("user", "simeon") \
#                     .option("password", "password") \
#                         .start().awaitTermination()



#streaming_df.dropna()
 



