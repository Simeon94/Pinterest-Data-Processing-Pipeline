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
#os.environ['PYSPARK_SUBMIT_ARGS'] = './bin/spark-class org.apache.spark.deploy.SparkSubmit'

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

# method to upload streaming_df to postgres database table
def write_streaming_df_to_postgres(df, epoch_id):

    df.write \
    .mode('append') \
    .format('jdbc') \
    .option('url', f'jdbc:postgresql://localhost:5432/postgres') \
    .option('driver', 'org.postgresql.Driver') \
    .option('dbtable', 'pinterest') \
    .option('user', 'postgres') \
    .option('password', 'postgres') \
    .save()
        
spark_api_data.writeStream\
    .foreachBatch(write_streaming_df_to_postgres) \
    .start() \
    .awaitTermination()
                    
spark.stop()