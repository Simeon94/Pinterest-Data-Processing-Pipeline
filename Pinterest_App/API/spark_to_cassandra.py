# script to cconfigure cassandra and send spark data to cassandra table

# Create a keyspace
CREATE KEYSPACE api_data WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = 'true';

# Create a table
CREATE TABLE api_data.pinterest_data  ( "index" int PRIMARY KEY, \
    category text, description text, downloaded int, follower_count int, \
    image_src text, is_image_or_video text, save_location text, tag_list text, title text, unique_id text, );

# write df staright to Cassandra table created above
dfout.write
  .format("org.apache.spark.sql.cassandra")
  .mode("overwrite")
  .option("confirm.truncate", "true")
  .option("spark.cassandra.connection.host", "ec2-34-253-182-40.eu-west-1.compute.amazonaws.com")
  .option("spark.cassandra.connection.port", "9042")
  .option("keyspace", "api_data")
  .option("table", "api_data.pinterest_data")
  .save()
