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