from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType, MapType, DataType
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, col, when
import os

# data_spark_schema = ArrayType(StructType([
#         StructField("category", StringType(), True), \
#         StructField("index", IntegerType(), True), \
#         StructField("unique_id", StringType(), True), \
#         StructField("title", StringType(), True), \
#         StructField("description", StringType(), True), \
#         StructField("follower_count", IntegerType(), True), \
#         StructField("tag_list", StringType(), True), \
#         StructField("is_image_or_video", StringType(), True), \
#         StructField("image_src", StringType(), True), \
#         StructField("downloaded", StringType(), True), \
#         StructField("save_location",StringType(), True)]))


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

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

#sc = spark.sparkContext

# Construct a streaming DataFrame that reads from topic
streaming_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
            .option("startingOffsets", "latest") \
                .load()
                
#streaming_df.printSchema()

# Select the value part of the kafka message and cast it to a string.
streaming_df1 = streaming_df.selectExpr("CAST(value AS STRING)")

data_spark_schema = ArrayType(StructType([
        T.StructField("category", T.StringType(), True), \
        T.StructField("index", T.StringType(), True), \
        T.StructField("unique_id", T.StringType(), True), \
        T.StructField("title", T.StringType(), True), \
        T.StructField("description", T.StringType(), True), \
        T.StructField("follower_count", T.StringType(), True), \
        T.StructField("tag_list", T.StringType(), True), \
        T.StructField("is_image_or_video", T.StringType(), True), \
        T.StructField("image_src", T.StringType(), True), \
        T.StructField("downloaded", T.StringType(), True), \
        T.StructField("save_location", T.StringType(), True)]))

# Access nested items of json; transforms the rows to the columns of the dataframe
streaming_df2  = streaming_df1.withColumn("temp", F.explode(F.from_json("value", data_spark_schema))).select("temp.*")
#streaming_df2.printSchema()

# Cleaning the data
streaming_df2 = (streaming_df2.replace({'No description available Story format':None}, subset=['description']) \
    .replace({'No Title Data Available':None}, subset=['title']) \
        .replace({'User Info Error':None}, subset=['follower_count']) \
            .replace({'Image src error':None}, subset=['image_src']) \
                .withColumn('save_location', regexp_replace('save_location', 'Local save in ', '')) \
                    .withColumn('follower_count', regexp_replace('follower_count', 'M', '000000')) \
                        .withColumn('follower_count', regexp_replace('follower_count', 'k', '000')) \
                            .replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e':None}, subset=['tag_list']) \
                                .withColumn('index', col('index').cast(IntegerType())) \
                                    .withColumn('follower_count', col('follower_count').cast(IntegerType())) \
                                        .withColumn('downloaded', col('downloaded').cast("integer")) \
                                            .distinct())

# replace empty cells with null
streaming_df3 = streaming_df2.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in streaming_df2.columns])

# reorder the dataframe columns
streaming_df4 = streaming_df3.select('index', 'unique_id', 'category', 'title', 'description', 'tag_list', 'downloaded', 'follower_count', 'is_image_or_video', 'image_src', 'save_location')

spark_api_data = streaming_df4

spark_api_data.printSchema()

# function to upload streaming_df to postgres database table
def write_streaming_df_to_postgres(df, epoch_id):
    mode='append'
    url = 'jdbc:postgresql://localhost:5432/pinterest'
    properties = {"user": "postgres", "password": "password", "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=url, table="pinterest", mode=mode, properties=properties)

spark_api_data.writeStream\
    .format("jdbc") \
        .foreachBatch(write_streaming_df_to_postgres) \
            .outputMode("append") \
                .start() \
                    .awaitTermination()
    

# outputting the messages to console
# streaming_df2.writeStream.format("console").outputMode("append").option("truncate", True).start().awaitTermination()

# Upload messages to postgres database table
# streaming_df2.writeStream.format("jdbc").outputMode("append") \
#     .option("url", "jdbc:postgresql://localhost:5432/pinterest") \
#         .option("driver", "org.postgresql.Driver") \
#             .option("dbtable", "pinterest") \
#                 .option("user", "simeon") \
#                     .option("password", "password") \
#                         .start().awaitTermination()

#streaming_df1 = streaming_df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# 
#def clean_data(dataframe):
#dataframe = streaming_df.selectExpr("CAST(value AS STRING)")

#dataframe = dataframe.withColumn("temp", F.explode(F.from_json("value", data_spark_schema))).select("temp.*")
#dataframe.printSchema()

#clean_data(streaming_df)
#dataframe.writeStream.outputMode("append").format("console").option("truncate", False).start().awaitTermination()
#streaming_df.writeStream.outputMode("append").foreachBatch(clean_data).format("console").option("truncate", False).start().awaitTermination()
 



