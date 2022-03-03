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
        self.consumer = KafkaConsumer('MyFirstKafkaTopic', bootstrap_servers='localhost:9092', group_id='my-group') #value_deserializer=lambda x: loads(x).decode('utf-8'))
        self.message = []
        self.output_dict = {}           
        self.s3_client = boto3.client('s3')
        #self.test = test
        

    # def consume(self):
    #     for message in self.consumer:
    #         api_event = message.value
    #         self.message.append(api_event)
    #         #print (self.message)
    
    def consume(self):
        for i in self.consumer:
            api_event = i.value
            self.message.append(api_event)
            #x = dict(i)
            for n in range(1, 200):
                out_file = open(f"api_data{n}.json", "w")
                json.dump(str(i), out_file)
                out_file.close()
                self.s3_client.upload_file(f'./api_data{n}.json', 'simeon-streaming-bucket', f'api_data{n}.json')
                print('message saved as json file and sent to s3')
                
            break
                
        for n in range(1, 200):
            os.remove(f'./api_data{n}.json')
            print('all api_data file deleted')
                
                
            
            # for j, v in enumerate(self.message):
            #     self.output_dict[j] = v
            #     print(type(self.output_dict))
                # y = json.dumps(self.output_dict)
                # print(y)
                # out_file = open("api_data.json", "w") #encoding='utf-8')
                # json.dumps(dict(self.output_dict)) #cls=MyEncoder, indent=4)
                # #json.dumps(output_dict, default=lambda o: o.__dict__, sort_keys=True, indent=4)
                # out_file.close()
                # self.s3_client.upload_file('./api_data.json', 'simeon-streaming-bucket', 'api_data')
                
# class MyEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, np.ndarray):
#             return obj.tolist()
#         elif isinstance(obj, bytes):
#             return str(obj, encoding='utf-8')
#         return json.JSONEncoder.default(self, obj)

    
# out_file = open("api_data.json", "w", encoding='utf-8')
# json.dumps(self.output_dict, out_file, cls=MyEncoder, indent=4)
# #json.dumps(output_dict, default=lambda o: o.__dict__, sort_keys=True, indent=4)
# out_file.close()
# self.s3_client.upload_file('./api_data.json', 'simeon-streaming-bucket', 'api_data')

    # def delete_files(self):
    #     for n in range(1, 100000):
    #         os.remove(f'./api_data{n}.json')
    #         print('all api_data files deleted')

    def run(self):
        '''
        This function is used to run or execute all the methods.
        '''
        self.consume()
        #self.save_api_json()
        #self.send_json_s3()

batch = streaming()
batch.run()