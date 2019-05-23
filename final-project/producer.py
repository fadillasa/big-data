from time import sleep
from json import dumps
from kafka import KafkaProducer
import os
import pandas as pd

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

dataset_folder_path = os.path.join(os.getcwd(), 'dataset')
dataset_file_path = os.path.join(dataset_folder_path, 'crime-data-final.csv')

data_limit = 1000       #jumlah data per model
counter = 0             #counter baris
model_limit = 3         #jumlah model

with open(dataset_file_path,"r", encoding="utf-8") as f:
    for row in f:
        if counter > data_limit*model_limit:
            break
        producer.send('crime', value=row)
        counter += 1
        print(row)
        sleep(0.000001)
