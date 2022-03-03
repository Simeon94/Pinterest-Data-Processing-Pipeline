import sys
from json import loads
from project_pin_API import Data
from kafka import KafkaConsumer

#consumer = KafkaConsumer('MyFirstKafkaTopic', bootstrap_servers='localhost:9092', group_id='my-group', value_deserializer=lambda x: loads(x).decode('utf-8'))
consumer = KafkaConsumer('MyFirstKafkaTopic', bootstrap_servers='localhost:9092', group_id='my-group')
for message in consumer:
    message = message.value
    print (message)

