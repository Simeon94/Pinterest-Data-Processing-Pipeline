import json
import boto3

s3_client = boto3.client('s3')

dict1 ={
            "emp1": {
                "name": "Lisa",
                "designation": "programmer",
                "age": "34",
                "salary": "54000"
            },
            "emp2": {
                "name": "Elis",
                "designation": "Trainee",
                "age": "24",
                "salary": "40000"
            },
        }
  
# the json file where the output must be stored
out_file = open("sample.json", "w")
  
json.dump(dict1, out_file, indent = 6)
  
out_file.close()
        
s3_client.upload_file('./sample.json', 'simeon-streaming-bucket', 'out_file.json')
