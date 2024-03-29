##Batch Streaming

# script to send data to AWS S3 bucket and publish df to cassandra

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import pandas as pd
import prestodb
import logging
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import regexp_replace, col, when

# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.2.0 pyspark-shell"

# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    
sc=SparkContext(conf=conf)

# # Create our Spark session
spark=SparkSession(sc).builder.appName("S3App").getOrCreate()
print("Working")

# Configure the setting to read from the S3 bucket
accessKeyId = os.environ["AWS_ACCESS_KEY"]
secretAccessKey = os.environ["AWS_SECRET_ACCESS_KEY"]

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS


try:
    df = spark.read.json(f"s3a://simeon-streaming-bucket/api_data/*.json")

    # # Cleaning the data
    df = (df.replace({'No description available Story format':None}, subset=['description']) \
        .replace({'No Title Data Available':None}, subset=['title']) \
        .replace({'User Info Error':None}, subset=['follower_count']) \
        .replace({'Image src error':None}, subset=['image_src']) \
        .withColumn('save_location', regexp_replace('save_location', 'Local save in ', '')) \
        .withColumn('follower_count', regexp_replace('follower_count', 'M', '000000')) \
        .withColumn('follower_count', regexp_replace('follower_count', 'k', '000')) \
        .replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e':None}, subset=['tag_list']) \
        .withColumn('index', col('index').cast("int")) \
        .withColumn('follower_count', col('follower_count').cast("int")) \
        .withColumn('downloaded', col('downloaded').cast("int")) \
        .distinct())

    # replace empty cells with null
    df = df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])

    # reorder the dataframe columns
    df = df.select('index', 'category', 'description', 'downloaded', 'follower_count', 'image_src', 'is_image_or_video', 'save_location', 'tag_list', 'title', 'unique_id')

    df.printSchema()
    df.show(truncate=True)

    df.write.format("org.apache.spark.sql.cassandra") \
        .mode("overwrite") \
        .option("confirm.truncate", "true") \
        .option("spark.cassandra.connection.host", "127.0.0.1") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "api_data") \
        .option("table", "pinterest_data2") \
        .save()

    spark.stop()
    #sys.exit()

    connection = prestodb.dbapi.connect(
        host='localhost',
        catalog='cassandra',
        user='Simeon',
        port=8080,
        schema='api_data'
    )

    cur = connection.cursor()
    cur.execute("SELECT * FROM pinterest_data2")
    rows = cur.fetchall()

    api_df = pd.DataFrame(rows)
    print(api_df)

except Exception as e:
    logging.basicConfig(filename="/home/ubuntu/airflow/error_log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.ERROR)
    logging.error(e, exc_info=True)

####Real time Streaming
# import packages required for executing the streaming job
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType, MapType, DataType
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, col, when
import os

# submit the spark sql package to PySpark during script execution
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.2.6 pyspark-shell'

# state the topic we want to stream the data from
kafka_topic_name = "MyFirstKafkaTopic"
kafka_bootstrap_servers = "localhost:9092"

# create a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL") \
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

# schema of the data from the source
data_spark_schema = ArrayType(StructType([
        StructField("category", StringType(), True), \
        StructField("index", IntegerType(), True), \
        StructField("unique_id", StringType(), True), \
        StructField("title", StringType(), True), \
        StructField("description", StringType(), True), \
        StructField("follower_count", StringType(), True), \
        StructField("tag_list", StringType(), True), \
        StructField("is_image_or_video", StringType(), True), \
        StructField("image_src", StringType(), True), \
        StructField("downloaded", StringType(), True), \
        StructField("save_location",StringType(), True)]))

# data_spark_schema = ArrayType(StructType([
#         T.StructField("category", T.StringType(), True), \
#         T.StructField("index", T.StringType(), True), \
#         T.StructField("unique_id", T.StringType(), True), \
#         T.StructField("title", T.StringType(), True), \
#         T.StructField("description", T.StringType(), True), \
#         T.StructField("follower_count", T.StringType(), True), \
#         T.StructField("tag_list", T.StringType(), True), \
#         T.StructField("is_image_or_video", T.StringType(), True), \
#         T.StructField("image_src", T.StringType(), True), \
#         T.StructField("downloaded", T.StringType(), True), \
#         T.StructField("save_location", T.StringType(), True)]))

# Select the value part of the kafka message and cast it to a string.
streaming_df1 = streaming_df.selectExpr("CAST(value AS STRING)")

# access nested items of json; transforms the rows to the columns of the dataframe
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

#streaming_df2.printSchema()
# replace empty cells with null
streaming_df3 = streaming_df2.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in streaming_df2.columns])

# reorder the dataframe columns
streaming_df4 = streaming_df3.select('index', 'unique_id', 'category', 'title', 'description', 'tag_list', 'downloaded', 'follower_count', 'is_image_or_video', 'image_src', 'save_location')

spark_api_data = streaming_df4

#spark_api_data.printSchema()

# function to upload streaming_df to postgres database table
def write_streaming_df_to_postgres(df, epoch_id):
    # mode='append'
    # url = 'jdbc:postgresql://localhost:5432/postgres'
    # properties = {"user": "postgres", "password": "password", "driver": "org.postgresql.Driver"}
    # df.write.jdbc(url=url, table="pinterest", mode=mode, properties=properties).save()

    df.write \
    .mode('append') \
    .format('jdbc') \
    .option('url', f'jdbc:postgresql://localhost:5432/postgres') \
    .option('driver', 'org.postgresql.Driver') \
    .option('dbtable', 'pinterest') \
    .option('user', 'postgres') \
    .option('password', 'password') \
    .save()
        
spark_api_data.writeStream\
    .foreachBatch(write_streaming_df_to_postgres) \
    .start() \
    .awaitTermination()
                    
spark.stop()
# spark_api_data.writeStream\
#     .foreachBatch(write_streaming_df_to_postgres)\
#         .start() \
#             .awaitTermination()
    

# outputting the messages to console
#spark_api_data.writeStream.format("console").outputMode("append").option("truncate", True).start().awaitTermination()  #This prints results to console