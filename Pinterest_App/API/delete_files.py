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
# accessKeyId = os.environ["AWS_ACCESS_KEY"]
# secretAccessKey = os.environ["AWS_SECRET_ACCESS_KEY"]

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', 'AKIARIZ4XLQ2MVTJRX47')
hadoopConf.set('fs.s3a.secret.key', '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')
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


########################################################################################################################################
# import sys
# import os
# import findspark
# #findspark.init()
# #import org.apache.spark.sql.cassandra._
# from json import loads
# from json import dumps
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession, SQLContext
# import boto3
# import pandas as pd
# from pyspark.sql.types import StructType,StructField,StringType
# from pyspark.sql import functions as F
# from pyspark.sql import types as T
# import prestodb
# import logging

# try:
    
#     s3 = boto3.resource('s3')
#     bucket=s3.Bucket('simeon-streaming-bucket')

#     os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 spark_s3_cassandra.py pyspark-shell'
    
#     spark = SparkSession.builder.master("local").appName("testapp").getOrCreate()
#     sc = spark.sparkContext

#     json_list = []

#     #v = len(list(bucket.objects.all()))
           
#     # for obj in range(v):
#     #     obj = s3.Object(bucket_name='simeon-streaming-bucket', key=f'api_data{i}.json').get()
#     #     obj_string_to_json = obj["Body"].read().decode('utf-8')
#     #     data = dumps(obj_string_to_json).replace("'", '"').rstrip('"').lstrip('"')
#     #     json_list.append(data)
        
#     for i in range(5):
#         obj = s3.Object(bucket_name='simeon-streaming-bucket', key=f'api_data{i}.json').get()
#         obj_string_to_json = obj["Body"].read().decode('utf-8')
#         data = dumps(obj_string_to_json).replace("'", '"').rstrip('"').lstrip('"')
#         json_list.append(data)
        
#     df = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json(sc.parallelize(json_list))
#     df = spark.read.option("mode", "DROPMALFORMED").option("columnNameOfCorruptRecord", "_corrupt_record").json(sc.parallelize(json_list))
    
#     # Cleaning the data
#     df = (df.replace({'No description available Story format':None}, subset=['description']) \
#         .replace({'No Title Data Available':None}, subset=['title']) \
#         .replace({'User Info Error':None}, subset=['follower_count']) \
#         .replace({'Image src error':None}, subset=['image_src']) \
#         .withColumn('save_location', regexp_replace('save_location', 'Local save in ', '')) \
#         .withColumn('follower_count', regexp_replace('follower_count', 'M', '000000')) \
#         .withColumn('follower_count', regexp_replace('follower_count', 'k', '000')) \
#         .replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e':None}, subset=['tag_list']) \
#         .withColumn('index', col('index').cast(IntegerType())) \
#         .withColumn('follower_count', col('follower_count').cast(IntegerType())) \
#         .withColumn('downloaded', col('downloaded').cast("integer")) \
#         .distinct())

#     # replace empty cells with null
#     df = df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])
    
#     # renames column for Cassandra table
#     #df = df.withColumnRenamed("index", "ind")

#     # reorder the dataframe columns
#     df = df.select('index', 'category', 'description', 'downloaded', 'follower_count', 'image_src', 'is_image_or_video', 'save_location', 'tag_list', 'title', 'unique_id')

#     df.printSchema()
#     type(df)
#     df.show(truncate=True)

#     #df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate", "true").option("spark.cassandra.connection.host", "localhost").option("spark.cassandra.connection.port", "9094").option("keyspace", "api_data").option("table", "api_data.pinterest_data").save()

#     df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate", "true").option("keyspace", "api_data").option("table", "pinterest_data").save()

#     spark.stop()
#     #sys.exit()

#     connection = prestodb.dbapi.connect(
#         host='localhost',
#         catalog='cassandra',
#         user='Simeon',
#         port=8080,
#         schema='api_data'
#     )

#     cur = connection.cursor()
#     cur.execute("SELECT * FROM pinterest_data")
#     rows = cur.fetchall()

#     api_df = pd.DataFrame(rows)
#     print(api_df)


# except Exception as e:
#     logging.basicConfig(filename="/home/ubuntu/airflow/error_log",
#                     filemode='a',
#                     format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
#                     datefmt='%H:%M:%S',
#                     level=logging.ERROR)
#     logging.error(e, exc_info=True)
