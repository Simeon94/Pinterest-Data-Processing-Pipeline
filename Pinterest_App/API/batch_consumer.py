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

    def __init__(self):
        # self.consumer = KafkaConsumer('MyFirstKafkaTopic', bootstrap_servers='localhost:9092', group_id='my-group') #value_deserializer=lambda x: loads(x).decode('utf-8'))
        # self.message = []
        # self.output_dict = {}
        # self.s3_client = boto3.client('s3')
        
        self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092', group_id='my-group', value_deserializer=lambda x: loads(x), auto_offset_reset = "earliest")
        self.consumer.subscribe(topics = "MyFirstKafkaTopic")
        self.message = []
        self.output_dict = {}
        #self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = "simeon-streaming-bucket"
        self.my_bucket = self.s3_resource.Bucket(self.bucket_name)

    
    def consume_to_s3(self):
        # for i in self.consumer:
        #     api_event = i.value
        #     self.message.append(api_event)
        #     #x = dict(i)
        #     for n in range(10):
        #         out_file = open(f"api_data{n}.json", "w")
        #         json.dump(str(i), out_file)
        #         out_file.close()
                #self.s3_client.upload_file(f'./api_data{n}.json', 'simeon-streaming-bucket', f'api_data{n}.json')
                #print('message saved as json file and sent to s3')
                

            #break
        # counter = 0
        # while counter <= 5:
            
        #     for i in self.consumer:
        #         api_event = i.value
        #         self.message.append(api_event)
        #         out_file = open(f"api_data" + str(counter) + ".json", "w")
        #         json.dump(str(i), out_file)
        #         out_file.close()
        #         self.s3_client.upload_file(f"./api_data" + str(counter) + ".json", 'simeon-streaming-bucket', f"api_data" + str(counter) + ".json")
        #         print("{0} saved as json file and sent to s3".format(out_file))
        #         counter += 1
        #         if counter > 5:
        #             break
                
            
        # for n in range(5):
        #     os.remove(f'./api_data{n}.json')
        #     print('all api_data file deleted')
        
        # send data from Consumer to S3 bucket
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
        This function is used to run or execute all the methods.
        '''
        self.consume_to_s3()
        #self.save_api_json()
        #self.send_json_s3()

batch = streaming()
batch.run()
