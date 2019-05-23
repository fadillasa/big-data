from kafka import KafkaConsumer
from json import loads
import os

consumer = KafkaConsumer(
    'crime',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

folder_path = os.path.join(os.getcwd(), 'dataset')
data_limit = 500000 #jumlah data per model
counter = 1         #counter baris
model_limit = 3     #jumlah model
model_number = 1

try:
    for message in consumer:
        if model_number > model_limit:
            writefile.close()
            break
        else:
            if counter > data_limit:
                counter = 1
                model_number += 1
                writefile.close()
            if model_number > model_limit:
                writefile.close()
                break
            if counter == 1:
                file_path = os.path.join(folder_path, ('model-' + str(model_number) + '.txt'))
                writefile = open(file_path, "w", encoding="utf-8")
            message = message.value
            writefile.write(message)
            print('current model : ' + str(model_number) + ' current data for this model : ' + str(counter))
            counter += 1
except KeyboardInterrupt:
    writefile.close()
print('Keyboard Interrupt called by user, exiting.....')