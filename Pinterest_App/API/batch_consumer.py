import time
import sys
import os
import json
from json import loads
from json import dumps
from project_pin_API import Data
from kafka import KafkaConsumer
import boto3 

class streaming:
    """"This class loads api data from kafka, \
        sends it to s3 bucketfor long term storage"""

    def __init__(self):        
        self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092', group_id='my-group', value_deserializer=lambda x: loads(x), auto_offset_reset = "earliest")
        self.consumer.subscribe(topics = "MyFirstKafkaTopic")
        #self.message = []
        #self.output_dict = {}
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = "simeon-streaming-bucket"
        self.my_bucket = self.s3_resource.Bucket(self.bucket_name)

    
    def consume_to_s3(self):
        '''
        This method is used to consume api data and each one is saved \
        in a json file which is then uploaded to s3 bucket.
        '''
        counter = 0
        for message in self.consumer:
            
            json_object = dumps(message.value)
            s3_apidata = 'api_data/' + str(counter) + '.json'
            self.my_bucket.put_object(Key = s3_apidata , Body = json_object)
            counter += 1
            if counter > 200:
                break

    def run(self):
        '''
        This method is used to run or execute all the methods of the class.
        '''
        self.consume_to_s3()

batch = streaming()
batch.run()
