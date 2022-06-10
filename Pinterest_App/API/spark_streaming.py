from pyspark.sql import SparkSession
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark_streaming.py pyspark-shell'
kafka_topic_name = "MyFirstKafkaTopic"
kafka_bootstrap_servers = "localhost:9092"

spark = SparkSession.builder.appName("Kafka").getOrCreate()
#sc = spark.sparkContext

streaming_df = spark.readStream.format("Kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", kafka_topic_name).option("startingOffsets", "earliest").load()

streaming_df.writeStream.outputMode("append").format("console").start().awaitTermination()

streaming_df.show(truncate=True)
