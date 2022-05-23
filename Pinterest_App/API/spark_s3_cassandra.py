# script to send data to AWS S3 bucket and publish df to cassandra

import os
import findspark
#findspark.init()
#import org.apache.spark.sql.cassandra._
from json import loads
from json import dumps
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import boto3
import pandas as pd
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql import functions as F
from pyspark.sql import types as T


s3 = boto3.resource('s3')
bucket=s3.Bucket('simeon-streaming-bucket')

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 spark_s3_cassandra.py pyspark-shell'

#./bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.1 spark_s3_cassandra.py

#./bin/spark-submit/pyspark --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.1 spark_s3_cassandra.py


#spark = SparkSession.builder.master("ec2-52.51.100.105.eu-west-1.compute.amazonaws.com").appName("testapp").getOrCreate()
#spark = SparkSession.builder \
  #.appName('SparkCassandraApp') \
  #.config('spark.cassandra.connection.host', 'ec2-52.51.100.105.eu-west-1.compute.amazonaws.com').config('spark.cassandra.connection.port', '9094').master('local').getOrCreate() 
  
spark = SparkSession.builder.master("local[2]").appName("testapp").getOrCreate()
sc = spark.sparkContext

# conf = SparkConf()

# conf.set("spark.cassandra.connection.host", "52.51.100.105")

# sc = SparkContext('spark://spark-hostname:9042',conf=conf)

# spark = SparkSession.builder.master("local").appName("testapp").getOrCreate()

# ss = SparkSession(sc)

#spark = SparkSession.builder.master("local").appName("testapp").getOrCreate()
#sc = spark.sparkContext

json_list = []

for i in range(10):
    obj = s3.Object(bucket_name='simeon-streaming-bucket', key=f'api_data{i}.json').get()
    obj_string_to_json = obj["Body"].read().decode('utf-8')
    data = dumps(obj_string_to_json).replace("'", '"').rstrip('"').lstrip('"')
    json_list.append(data)
    
df = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json(sc.parallelize(json_list))

type(df)
df.show(truncate=True)

#df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate", "true").option("spark.cassandra.connection.host", "localhost").option("spark.cassandra.connection.port", "9094").option("keyspace", "api_data").option("table", "api_data.pinterest_data").save()

df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate", "true").option("keyspace", "api_data").option("table", "pinterest_data").save()